from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

from cache import read_cache, write_cache
from market_universe import infer_market_region, infer_market_session, latest_market_observation


log = logging.getLogger("smallcap_scanner")

BASE_DIR = Path(__file__).resolve().parent

SMALLCAP_CACHE_KEY = "smallcap_ideas"
SMALLCAP_META_CACHE_KEY = "smallcap_ideas_meta"

DEFAULT_MAX_MARKET_CAP = 2_000_000_000
DEFAULT_MIN_MARKET_CAP = 30_000_000
DEFAULT_MIN_PRICE = 1.0
DEFAULT_MAX_PRICE = 20.0
DEFAULT_MIN_AVG_VOLUME = 120_000
DEFAULT_MIN_DAY_VOLUME = 200_000
DEFAULT_MAX_RESULTS = 30
DEFAULT_MAX_CANDIDATES = 220

EXCLUDED_PATTERN = re.compile(
    r"Warrant|Rights?|Units?|Preferred|Depositary|Trust Preferred|Acquisition Corp|ETF|Fund",
    re.IGNORECASE,
)

FALLBACK_TICKERS = [
    "ACHR", "AMPX", "ARBE", "ASTS", "BBAI", "BITF", "BLNK", "BTBT", "CIFR", "CLSK",
    "CRDO", "DNA", "ENVX", "EOSE", "EVGO", "HIMS", "HUT", "IONQ", "JOBY", "LUNR",
    "MARA", "MVST", "NNOX", "OPEN", "PLUG", "QBTS", "RKLB", "RUM", "SANA", "SOUN",
    "TMC", "UPST", "WULF",
]


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _to_series(values: Any) -> pd.Series:
    if isinstance(values, pd.DataFrame):
        col = "Close" if "Close" in values.columns else values.columns[0]
        values = values[col]
        if isinstance(values, pd.DataFrame):
            values = values.iloc[:, 0]
    if isinstance(values, pd.Series):
        return values.astype(float).dropna()
    return pd.Series(values, dtype=float).dropna()


def _normalize_history_frame(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    result = df.copy()
    if isinstance(result.columns, pd.MultiIndex):
        target = str(ticker or "").upper()
        if target:
            for level in range(result.columns.nlevels):
                match = None
                for value in result.columns.get_level_values(level).unique():
                    if str(value).upper() == target:
                        match = value
                        break
                if match is not None:
                    try:
                        result = result.xs(match, axis=1, level=level, drop_level=True)
                        break
                    except Exception:
                        pass

        if isinstance(result.columns, pd.MultiIndex):
            price_labels = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
            for level in range(result.columns.nlevels):
                values = {str(v) for v in result.columns.get_level_values(level).unique()}
                if values & price_labels:
                    result.columns = result.columns.get_level_values(level)
                    break

    if isinstance(result.columns, pd.MultiIndex):
        return pd.DataFrame()

    result = result.loc[:, ~pd.Index(result.columns).duplicated()]
    wanted = [col for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if col in result.columns]
    return result[wanted].dropna(how="all") if wanted else pd.DataFrame()


def _format_money(value: float | int | None) -> str:
    value = _to_float(value)
    if value is None:
        return "-"
    if abs(value) >= 1e9:
        return f"{value / 1e9:.1f}B$"
    if abs(value) >= 1e6:
        return f"{value / 1e6:.1f}M$"
    if abs(value) >= 1e3:
        return f"{value / 1e3:.0f}K$"
    return f"{value:.0f}$"


def _format_volume(value: float | int | None) -> str:
    value = _to_float(value)
    if value is None:
        return "-"
    if abs(value) >= 1e9:
        return f"{value / 1e9:.1f}B"
    if abs(value) >= 1e6:
        return f"{value / 1e6:.1f}M"
    if abs(value) >= 1e3:
        return f"{value / 1e3:.0f}K"
    return str(int(value))


def _calc_rsi(prices: pd.Series, period: int = 14) -> float | None:
    closes = _to_series(prices)
    if len(closes) < period + 1:
        return None
    delta = closes.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    value = _to_float(rsi.iloc[-1])
    return round(value, 1) if value is not None else None


def _screen_candidates(
    max_market_cap: int,
    min_market_cap: int,
    min_price: float,
    max_price: float,
    min_day_volume: int,
    max_candidates: int,
) -> tuple[list[dict], dict[str, int]]:
    candidate_map: dict[str, dict] = {}
    source_counts: dict[str, int] = {}

    screen_specs = [
        ("smallcap_movers", "percentchange", max_candidates),
        ("smallcap_volume", "dayvolume", max_candidates),
        ("smallcap_marketcap", "intradaymarketcap", max_candidates // 2),
    ]
    filters = [
        yf.EquityQuery("eq", ["region", "us"]),
        yf.EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
        yf.EquityQuery("gte", ["intradaymarketcap", min_market_cap]),
        yf.EquityQuery("lte", ["intradaymarketcap", max_market_cap]),
        yf.EquityQuery("gte", ["intradayprice", min_price]),
        yf.EquityQuery("lte", ["intradayprice", max_price]),
        yf.EquityQuery("gt", ["dayvolume", min_day_volume]),
    ]

    for label, sort_field, size in screen_specs:
        try:
            payload = yf.screen(
                yf.EquityQuery("and", filters),
                size=max(25, size),
                sortField=sort_field,
                sortAsc=False,
            )
            quotes = payload.get("quotes", []) if isinstance(payload, dict) else []
        except Exception as exc:
            log.warning("smallcap screen %s failed: %s", label, exc)
            quotes = []
        source_counts[label] = len(quotes)
        for quote in quotes:
            ticker = str(quote.get("symbol") or "").upper()
            name = str(quote.get("shortName") or quote.get("longName") or ticker)
            if not ticker or EXCLUDED_PATTERN.search(name):
                continue
            existing = candidate_map.setdefault(ticker, {"symbol": ticker, "_sources": []})
            existing.update({key: value for key, value in quote.items() if value is not None})
            existing["_sources"] = list(dict.fromkeys(existing.get("_sources", []) + [label]))

    if len(candidate_map) < 40:
        cached = read_cache("stock_ideas") or {}
        for row in cached.get("data", []) or []:
            ticker = str(row.get("Ticker") or row.get("ticker") or "").upper()
            cap = _to_float(row.get("cap_raw") or row.get("_market_cap_raw"))
            price = _to_float(row.get("Cours") or row.get("price"))
            if not ticker or cap is None or price is None:
                continue
            if min_market_cap <= cap <= max_market_cap and min_price <= price <= max_price:
                existing = candidate_map.setdefault(ticker, {"symbol": ticker, "_sources": []})
                existing.update(
                    {
                        "symbol": ticker,
                        "shortName": row.get("Nom") or ticker,
                        "regularMarketPrice": price,
                        "regularMarketChangePercent": row.get("Variation (%)"),
                        "marketCap": cap,
                    }
                )
                existing["_sources"] = list(dict.fromkeys(existing.get("_sources", []) + ["stock_ideas_cache"]))

    for ticker in FALLBACK_TICKERS:
        existing = candidate_map.setdefault(ticker, {"symbol": ticker, "_sources": []})
        existing["_sources"] = list(dict.fromkeys(existing.get("_sources", []) + ["fallback_watchlist"]))

    candidates = list(candidate_map.values())[:max_candidates]
    source_counts["dedup_candidates"] = len(candidates)
    return candidates, source_counts


def _fetch_histories(tickers: list[str], period: str = "3mo") -> dict[str, pd.DataFrame]:
    unique = list(dict.fromkeys(str(t).upper() for t in tickers if t))
    histories: dict[str, pd.DataFrame] = {ticker: pd.DataFrame() for ticker in unique}
    for start in range(0, len(unique), 40):
        chunk = unique[start:start + 40]
        try:
            raw = yf.download(
                " ".join(chunk),
                period=period,
                progress=False,
                auto_adjust=True,
                group_by="ticker",
                threads=False,
            )
            for ticker in chunk:
                histories[ticker] = _normalize_history_frame(raw, ticker)
        except Exception as exc:
            log.warning("smallcap history batch failed: %s", exc)
        for ticker in chunk:
            if histories.get(ticker, pd.DataFrame()).empty:
                try:
                    raw_one = yf.download(ticker, period=period, progress=False, auto_adjust=True, threads=False)
                    histories[ticker] = _normalize_history_frame(raw_one, ticker)
                except Exception:
                    histories[ticker] = pd.DataFrame()
    return histories


def _fast_info_quote(ticker: str) -> dict:
    try:
        fast = yf.Ticker(ticker).fast_info or {}
    except Exception:
        fast = {}

    def get(*keys: str) -> Any:
        for key in keys:
            try:
                value = fast.get(key)
            except Exception:
                value = None
            if value is not None:
                return value
        return None

    return {
        "marketCap": get("marketCap", "market_cap"),
        "regularMarketPrice": get("lastPrice", "last_price"),
        "averageDailyVolume3Month": get("threeMonthAverageVolume", "tenDayAverageVolume"),
    }


def _enrich_missing_quotes(candidates: list[dict]) -> list[dict]:
    enriched = [dict(candidate) for candidate in candidates]
    missing = [
        idx for idx, row in enumerate(enriched)
        if row.get("marketCap") is None or row.get("regularMarketPrice") is None
    ]
    if not missing:
        return enriched
    with ThreadPoolExecutor(max_workers=6) as pool:
        future_map = {pool.submit(_fast_info_quote, enriched[idx]["symbol"]): idx for idx in missing}
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                extra = future.result()
            except Exception:
                extra = {}
            for key, value in extra.items():
                if enriched[idx].get(key) is None and value is not None:
                    enriched[idx][key] = value
    return enriched


def _score_smallcap(quote: dict, hist: pd.DataFrame, settings: dict) -> dict | None:
    ticker = str(quote.get("symbol") or "").upper()
    if not ticker:
        return None

    name = str(quote.get("shortName") or quote.get("longName") or ticker)
    if EXCLUDED_PATTERN.search(name):
        return None

    if hist.empty or "Close" not in hist.columns or len(hist) < 25:
        return None

    closes = _to_series(hist["Close"])
    highs = _to_series(hist["High"]) if "High" in hist.columns else closes
    lows = _to_series(hist["Low"]) if "Low" in hist.columns else closes
    opens = _to_series(hist["Open"]) if "Open" in hist.columns else closes
    volumes = _to_series(hist["Volume"]) if "Volume" in hist.columns else pd.Series(dtype=float)
    if len(closes) < 25 or len(volumes) < 20:
        return None

    price = _to_float(quote.get("regularMarketPrice") or quote.get("intradayprice"), float(closes.iloc[-1]))
    previous = float(closes.iloc[-2]) if len(closes) >= 2 else price
    change_pct = _to_float(quote.get("regularMarketChangePercent") or quote.get("percentchange"))
    if change_pct is None and previous:
        change_pct = (float(price) / previous - 1) * 100

    market_cap = _to_float(quote.get("marketCap") or quote.get("intradaymarketcap"))
    volume = _to_float(quote.get("regularMarketVolume") or quote.get("dayvolume"), float(volumes.iloc[-1]))
    avg_volume_20d = float(volumes.tail(20).mean())
    if _to_float(quote.get("averageDailyVolume3Month") or quote.get("averageDailyVolume10Day")):
        avg_volume_20d = min(avg_volume_20d, float(_to_float(quote.get("averageDailyVolume3Month") or quote.get("averageDailyVolume10Day"))))

    min_price = settings["min_price"]
    max_price = settings["max_price"]
    min_market_cap = settings["min_market_cap"]
    max_market_cap = settings["max_market_cap"]
    min_avg_volume = settings["min_avg_volume"]
    min_day_volume = settings["min_day_volume"]

    if price is None or change_pct is None or market_cap is None:
        return None
    if not (min_price <= price <= max_price):
        return None
    if not (min_market_cap <= market_cap <= max_market_cap):
        return None
    if avg_volume_20d < min_avg_volume or volume < min_day_volume:
        return None

    rel_volume = volume / avg_volume_20d if avg_volume_20d > 0 else 0.0
    ma20 = float(closes.tail(20).mean())
    distance_ma20 = (price - ma20) / ma20 * 100 if ma20 else 0.0
    rsi_14 = _calc_rsi(closes)
    day_high = _to_float(quote.get("regularMarketDayHigh") or quote.get("dayHigh"), float(highs.iloc[-1]))
    day_low = _to_float(quote.get("regularMarketDayLow") or quote.get("dayLow"), float(lows.iloc[-1]))
    close_vs_day_high = price / day_high if day_high and day_high > 0 else None
    day_range_pct = (day_high - day_low) / price * 100 if day_high and day_low and price else 0.0
    intraday_position = (price - day_low) / (day_high - day_low) if day_high and day_low and day_high > day_low else 0.5
    high_20d_prev = float(highs.iloc[:-1].tail(20).max()) if len(highs) >= 22 else float(highs.tail(20).max())
    high_60d_prev = float(highs.iloc[:-1].tail(60).max()) if len(highs) >= 62 else float(highs.tail(min(len(highs), 60)).max())
    perf_3d = (price / float(closes.iloc[-4]) - 1) * 100 if len(closes) >= 4 and closes.iloc[-4] else 0.0
    perf_5d = (price / float(closes.iloc[-6]) - 1) * 100 if len(closes) >= 6 and closes.iloc[-6] else 0.0
    prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else float(price)
    gap_up = bool(opens.iloc[-1] > prev_close * 1.04) if len(opens) >= 2 and prev_close else False
    true_range = pd.concat(
        [
            highs - lows,
            (highs - closes.shift(1)).abs(),
            (lows - closes.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    volatility = float(true_range.tail(14).mean() / price * 100) if price else 0.0

    breakout = price >= high_20d_prev * 0.995 if high_20d_prev else False
    major_breakout = price >= high_60d_prev * 0.995 if high_60d_prev else False
    first_move = change_pct >= 8 and perf_5d < change_pct + 4
    continuation = perf_3d >= 8 and change_pct >= 2 and price >= ma20
    overextended = distance_ma20 >= 35 or (rsi_14 is not None and rsi_14 >= 88 and distance_ma20 >= 22)
    momentum_confirme = bool(rel_volume >= 1.5 and change_pct >= 5)
    faible_confirmation_volume = rel_volume < 1.2
    news_candidate = bool(rel_volume >= 3.0 and abs(change_pct) >= 8)

    # Un scanner "explosif" doit exiger au moins un debut de participation.
    # Sous 0.8x, le mouvement ressemble trop souvent a une hausse isolee.
    if rel_volume < 0.8:
        return None

    score = 0.0
    if rel_volume >= 5.0:
        score += 3.6
    elif rel_volume >= 3.0:
        score += 3.0
    elif rel_volume >= 2.0:
        score += 1.9
    elif rel_volume >= 1.2:
        score += 0.7
    elif rel_volume >= 1.0:
        score += 0.2
    else:
        score -= 1.4

    score += min(max(change_pct, 0), 35.0) * 0.06
    if close_vs_day_high is not None:
        if close_vs_day_high >= 0.98:
            score += 1.4
        elif close_vs_day_high >= 0.95:
            score += 0.9
        elif close_vs_day_high < 0.86:
            score -= 0.8
    if intraday_position >= 0.75:
        score += 0.6
    if breakout:
        score += 1.2
    if major_breakout:
        score += 0.8
    if continuation:
        score += 0.9
    if first_move:
        score += 0.6
    if gap_up:
        score += 0.4
    if momentum_confirme:
        score += 0.7
        if breakout or continuation:
            score += 0.4
    elif change_pct >= 10:
        score -= 0.4
    if 2.0 <= price <= 12.0:
        score += 0.4
    if volume < min_day_volume * 1.5:
        score -= 0.8
    if rel_volume < 1.2:
        score -= 1.0
        if change_pct < 8:
            score -= 0.8
    if rel_volume < 1.0:
        score -= 1.2
    if day_range_pct > 45 and close_vs_day_high is not None and close_vs_day_high < 0.9:
        score -= 1.0
    if volatility > 28 and rel_volume < 2:
        score -= 0.7
    if overextended:
        score -= 0.3

    explosion_score = round(min(max(score, 0.0), 10.0), 1)
    if explosion_score < 3.0:
        return None

    tags = []
    if momentum_confirme:
        tags.append("momentum_confirme")
    else:
        tags.append("speculatif")
    if faible_confirmation_volume:
        tags.append("faible_confirmation_volume")
    if first_move:
        tags.append("first_move")
    if continuation:
        tags.append("continuation")
    if overextended:
        tags.append("overextended")
    if news_candidate:
        tags.append("news_candidate")
    if gap_up:
        tags.append("gap_up")

    if major_breakout:
        setup = "breakout 60j"
    elif breakout:
        setup = "breakout 20j"
    elif continuation:
        setup = "continuation momentum"
    elif first_move:
        setup = "premier mouvement"
    else:
        setup = "volume spike"

    if momentum_confirme and (breakout or continuation):
        signal_quality = "momentum_confirme"
    elif faible_confirmation_volume:
        signal_quality = "faible_confirmation_volume"
    else:
        signal_quality = "speculatif"

    if volatility >= 22 or day_range_pct >= 35 or overextended:
        risk = "Tres eleve"
    elif volatility >= 13 or rel_volume >= 8 or abs(change_pct) >= 20:
        risk = "Eleve"
    else:
        risk = "Speculatif"

    reasons = []
    if momentum_confirme and (breakout or continuation):
        reasons.append("breakout avec volume anormal")
    elif momentum_confirme:
        reasons.append("hausse avec participation elevee")
    elif faible_confirmation_volume:
        reasons.append("hausse forte mais volume peu confirme")

    if rel_volume >= 3:
        reasons.append(f"volume relatif x{rel_volume:.1f}")
    elif rel_volume >= 1.2:
        reasons.append(f"volume relatif x{rel_volume:.1f}")
    elif faible_confirmation_volume:
        reasons.append(f"volume relatif faible x{rel_volume:.1f}")
    if change_pct >= 8:
        reasons.append(f"variation {change_pct:+.1f}%")
    if close_vs_day_high is not None and close_vs_day_high >= 0.95:
        reasons.append("cloture proche du plus haut")
    if breakout:
        reasons.append("cassure de range")
    if continuation:
        reasons.append("continuation multi-jours")
    if gap_up:
        reasons.append("gap haussier")
    if not reasons:
        reasons.append("activite inhabituelle")
    comment = ", ".join(reasons[:4])

    last_market_timestamp, last_observed_price = latest_market_observation(hist)
    market_region = infer_market_region(ticker)

    return {
        "ticker": ticker,
        "name": name,
        "price": round(float(price), 4),
        "change_pct": round(float(change_pct), 2),
        "market_cap": float(market_cap),
        "volume": float(volume),
        "avg_volume_20d": round(float(avg_volume_20d), 0),
        "rel_volume": round(float(rel_volume), 2),
        "rsi_14": rsi_14,
        "distance_from_ma20_pct": round(float(distance_ma20), 2),
        "close_vs_day_high": round(float(close_vs_day_high), 3) if close_vs_day_high is not None else None,
        "volatility": round(float(volatility), 2),
        "setup": setup,
        "risk": risk,
        "signal_quality": signal_quality,
        "comment": comment,
        "market_region": market_region,
        "market_session": infer_market_session(ticker),
        "last_market_timestamp": last_market_timestamp,
        "last_observed_price": round(float(last_observed_price), 4) if last_observed_price is not None else round(float(price), 4),
        "Explosion_Score": explosion_score,
        "tags": tags,
        "first_move": first_move,
        "continuation": continuation,
        "overextended": overextended,
        "momentum_confirme": momentum_confirme,
        "faible_confirmation_volume": faible_confirmation_volume,
        "news_candidate": news_candidate,
        "gap_up": gap_up,
        "day_range_pct": round(float(day_range_pct), 2),
        "intraday_position": round(float(intraday_position), 3),
        "sources": quote.get("_sources") or [],
        "Capitalisation": _format_money(market_cap),
        "Volume": _format_volume(volume),
    }


def scan_small_cap_opportunities(
    *,
    max_market_cap: int = DEFAULT_MAX_MARKET_CAP,
    min_market_cap: int = DEFAULT_MIN_MARKET_CAP,
    min_price: float = DEFAULT_MIN_PRICE,
    max_price: float = DEFAULT_MAX_PRICE,
    min_avg_volume: int = DEFAULT_MIN_AVG_VOLUME,
    min_day_volume: int = DEFAULT_MIN_DAY_VOLUME,
    max_results: int = DEFAULT_MAX_RESULTS,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> list[dict]:
    settings = {
        "max_market_cap": max_market_cap,
        "min_market_cap": min_market_cap,
        "min_price": min_price,
        "max_price": max_price,
        "min_avg_volume": min_avg_volume,
        "min_day_volume": min_day_volume,
    }
    candidates, source_counts = _screen_candidates(
        max_market_cap=max_market_cap,
        min_market_cap=min_market_cap,
        min_price=min_price,
        max_price=max_price,
        min_day_volume=min_day_volume,
        max_candidates=max_candidates,
    )
    candidates = _enrich_missing_quotes(candidates)
    tickers = [str(row.get("symbol") or "").upper() for row in candidates if row.get("symbol")]
    histories = _fetch_histories(tickers, period="3mo")

    rows = []
    for quote in candidates:
        ticker = str(quote.get("symbol") or "").upper()
        row = _score_smallcap(quote, histories.get(ticker, pd.DataFrame()), settings)
        if row:
            rows.append(row)

    rows.sort(
        key=lambda row: (
            float(row.get("Explosion_Score") or 0),
            float(row.get("rel_volume") or 0),
            float(row.get("change_pct") or 0),
        ),
        reverse=True,
    )
    for rank, row in enumerate(rows[:max_results], start=1):
        row["rank"] = rank
    scan_small_cap_opportunities.last_meta = {
        "calculated_at": utc_now_iso(),
        "candidate_count": len(candidates),
        "scored_count": len(rows),
        "returned_count": min(len(rows), max_results),
        "source_counts": source_counts,
        "filters": settings,
    }
    return rows[:max_results]


scan_small_cap_opportunities.last_meta = {}


def save_smallcap_results(results: list[dict], meta: dict | None = None) -> None:
    payload_meta = {
        **(getattr(scan_small_cap_opportunities, "last_meta", {}) or {}),
        **(meta or {}),
        "saved_at": utc_now_iso(),
    }
    write_cache(SMALLCAP_CACHE_KEY, results)
    write_cache(SMALLCAP_META_CACHE_KEY, payload_meta)
