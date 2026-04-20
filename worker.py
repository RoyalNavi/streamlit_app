#!/usr/bin/env python3
"""
Background worker — tourne en permanence sur la VM, indépendamment de Streamlit.
Gère par systemd : deploy/worker.service

Jobs :
  - toutes les 5 min  : prix / volumes / movers
  - toutes les 30 min : scoring avancé des actions (indicateurs techniques + RS)
  - 7h chaque matin   : contexte briefing pré-calculé
  - 18h chaque soir   : snapshot historique portefeuille
  - dimanche 2h       : nettoyage / rotation des vieux caches
"""

import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPool
from apscheduler.schedulers.blocking import BlockingScheduler

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from cache import write_cache, read_cache

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BASE_DIR / "data" / "worker.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SECTOR_ETFS = {
    "Technologie": "XLK",
    "Finance": "XLF",
    "Energie": "XLE",
    "Sante": "XLV",
    "Conso discr.": "XLY",
    "Industrie": "XLI",
    "Materiaux": "XLB",
    "Immobilier": "XLRE",
    "Services pub.": "XLU",
    "Conso base": "XLP",
}

EXCLUDED_PATTERN = (
    r"Warrant|Rights?|Units?|Preferred|Depositary|Trust Preferred|Acquisition Corp"
)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ---------------------------------------------------------------------------
# Technical indicators (pandas / numpy, sans dépendance externe)
# ---------------------------------------------------------------------------


def _to_series(prices) -> pd.Series:
    if isinstance(prices, pd.DataFrame):
        col = "Close" if "Close" in prices.columns else prices.columns[0]
        result = prices[col]
        # Duplicate column names can make prices[col] return a DataFrame
        if isinstance(result, pd.DataFrame):
            result = result.iloc[:, 0]
        return result.astype(float).dropna()
    if isinstance(prices, pd.Series):
        return prices.astype(float).dropna()
    return pd.Series(prices, dtype=float).dropna()


def calc_rsi(prices, period: int = 14) -> float:
    s = _to_series(prices)
    if len(s) < period + 1:
        return 50.0
    delta = s.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    try:
        val = float(rsi.iloc[-1])
        return val if not np.isnan(val) else 50.0
    except Exception:
        return 50.0


def calc_macd_signal(prices, fast: int = 12, slow: int = 26, signal: int = 9) -> str:
    s = _to_series(prices)
    if len(s) < slow + signal:
        return "neutre"
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    try:
        h1, h2 = float(hist.iloc[-1]), float(hist.iloc[-2])
    except Exception:
        return "neutre"
    if h1 > 0 and h2 <= 0:
        return "croisement_haussier"
    if h1 > 0:
        return "haussier"
    if h1 < 0 and h2 >= 0:
        return "croisement_baissier"
    return "baissier"


def calc_bollinger_position(prices, period: int = 20, std: float = 2.0) -> str:
    s = _to_series(prices)
    if len(s) < period:
        return "neutre"
    mid = s.rolling(period).mean()
    dev = s.rolling(period).std()
    upper = mid + std * dev
    lower = mid - std * dev
    try:
        price = float(s.iloc[-1])
        mid_val = float(mid.iloc[-1])
        upper_val = float(upper.iloc[-1])
        lower_val = float(lower.iloc[-1])
    except Exception:
        return "neutre"
    if price > upper_val:
        return "extension"
    if price >= mid_val:
        return "zone_haussiere"
    if price >= lower_val:
        return "zone_baissiere"
    return "sous_bande"


def calc_trend_quality(prices, period: int = 30) -> tuple[float, float]:
    """Retourne (R², pente_pct_par_jour)"""
    s = _to_series(prices)
    if len(s) < period:
        return 0.0, 0.0
    # .ravel() ensures 1D even if _to_series returns a 2-col slice
    y = np.asarray(s.iloc[-period:].values, dtype=float).ravel()
    if len(y) < period:
        return 0.0, 0.0
    x = np.arange(len(y), dtype=float)
    # Explicit OLS to avoid np.polyfit/polyval broadcasting issues
    n = float(len(x))
    sx, sy = x.sum(), y.sum()
    sxy = (x * y).sum()
    sxx = (x * x).sum()
    denom = n * sxx - sx * sx
    slope = float((n * sxy - sx * sy) / denom) if denom != 0 else 0.0
    intercept = float((sy - slope * sx) / n)
    y_pred = slope * x + intercept
    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = max(0.0, 1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0
    slope_pct = (slope / float(y.mean()) * 100) if y.mean() != 0 else 0.0
    return round(r2, 3), round(slope_pct, 3)


def calc_relative_strength(stock_prices, spy_prices, period: int) -> float | None:
    """Outperformance en % sur `period` jours vs SPY."""
    s = _to_series(stock_prices)
    spy = _to_series(spy_prices)
    if len(s) < period or len(spy) < period:
        return None
    try:
        stock_ret = (float(s.iloc[-1]) / float(s.iloc[-period]) - 1) * 100
        spy_ret = (float(spy.iloc[-1]) / float(spy.iloc[-period]) - 1) * 100
        return float(round(stock_ret - spy_ret, 2))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Screen helper (sans Streamlit)
# ---------------------------------------------------------------------------

import re

_EXCL = re.compile(EXCLUDED_PATTERN, re.IGNORECASE)


def _screen(sort_field: str = "percentchange", cap_min: int = 500_000_000, size: int = 100) -> list[dict]:
    filters = [
        yf.EquityQuery("eq", ["region", "us"]),
        yf.EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
        yf.EquityQuery("gte", ["intradaymarketcap", cap_min]),
        yf.EquityQuery("gte", ["intradayprice", 5]),
        yf.EquityQuery("gt", ["dayvolume", 300_000]),
    ]
    try:
        payload = yf.screen(yf.EquityQuery("and", filters), size=size, sortField=sort_field, sortAsc=False)
        return [q for q in payload.get("quotes", []) if q.get("symbol") and not _EXCL.search(str(q.get("shortName") or ""))]
    except Exception as e:
        log.warning(f"_screen({sort_field}) error: {e}")
        return []


def build_candidate_universe(spy_hist: pd.DataFrame) -> tuple[list[dict], dict[str, pd.DataFrame]]:
    """
    Construit un univers en 3 parties :
      40 top movers du jour
    + 40 meilleurs momentum 1m vs SPY (sur un univers large)
    + jusqu'à 20 tickers de la watchlist persistante
    Retourne (quotes_dict_ordered, histories)
    """
    # --- Partie 1 : top 40 movers du jour ---
    # --- Partie 1 : top movers par segment (univers équilibré) ---
    # Un screen global biaiserait vers les large caps (plus liquides, plus couvertes).
    # On screen explicitement chaque segment pour avoir de vraies meilleures small/mid/large.
    seg_screens = [
        _screen("percentchange", cap_min=300_000_000,    size=50),   # Small
        _screen("percentchange", cap_min=2_000_000_000,  size=50),   # Mid
        _screen("percentchange", cap_min=10_000_000_000, size=50),   # Large
    ]
    movers_quotes = [q for batch in seg_screens for q in batch]
    movers_tickers = list(dict.fromkeys(q["symbol"] for q in movers_quotes))[:60]

    # --- Partie 2 : momentum 1m (univers large trié par volume pour diversifier) ---
    broad_quotes = _screen("regularMarketVolume", cap_min=500_000_000, size=100)
    broad_tickers = [q["symbol"] for q in broad_quotes]

    # --- Partie 3 : watchlist persistante ---
    wl = read_cache("watchlist") or {"data": []}
    watchlist_tickers = [str(t).upper() for t in (wl.get("data") or []) if t][:20]

    # Univers brut (dédupliqué)
    all_tickers = list(dict.fromkeys(movers_tickers + broad_tickers + watchlist_tickers))
    all_quotes_map = {q["symbol"]: q for q in movers_quotes + broad_quotes}

    # Fetch histories en parallèle (4 threads)
    log.info(f"build_candidate_universe — fetch historiques pour {len(all_tickers)} tickers")
    histories: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        future_map = {pool.submit(_fetch_history, t, "3mo"): t for t in all_tickers}
        for fut in as_completed(future_map):
            t = future_map[fut]
            try:
                histories[t] = fut.result()
            except Exception:
                histories[t] = pd.DataFrame()

    # Calculer RS 1m pour chaque ticker → top 40 momentum
    def _rs1m(t: str) -> float:
        h = histories.get(t, pd.DataFrame())
        if h.empty or "Close" not in h.columns or spy_hist.empty or "Close" not in spy_hist.columns:
            return -999.0
        rs = calc_relative_strength(h["Close"], spy_hist["Close"], 21)
        if rs is None:
            return -999.0
        try:
            return float(rs)
        except Exception:
            return -999.0

    momentum_ranked = sorted(all_tickers, key=_rs1m, reverse=True)[:40]

    # Construire l'univers final ordonné (movers d'abord, puis momentum, puis watchlist)
    universe_tickers: list[str] = []
    seen: set[str] = set()
    for t in movers_tickers + momentum_ranked + watchlist_tickers:
        if t not in seen:
            universe_tickers.append(t)
            seen.add(t)

    # Pour les tickers watchlist absents du screen, créer un quote minimal depuis l'historique
    for t in watchlist_tickers:
        if t not in all_quotes_map:
            h = histories.get(t, pd.DataFrame())
            if not h.empty and "Close" in h.columns and len(h) >= 2:
                price = float(h["Close"].iloc[-1])
                prev = float(h["Close"].iloc[-2])
                all_quotes_map[t] = {
                    "symbol": t, "shortName": t,
                    "regularMarketPrice": price,
                    "regularMarketChangePercent": (price / prev - 1) * 100,
                    "marketCap": 0, "regularMarketVolume": 0,
                }

    # Tagguer la source pour transparence
    movers_set = set(movers_tickers)
    momentum_set = set(momentum_ranked)
    wl_set = set(watchlist_tickers)
    universe_quotes = []
    for t in universe_tickers:
        if t not in all_quotes_map:
            continue
        q = dict(all_quotes_map[t])
        src = []
        if t in movers_set: src.append("movers")
        if t in momentum_set: src.append("momentum1m")
        if t in wl_set: src.append("watchlist")
        q["_source"] = "+".join(src) or "screen"
        universe_quotes.append(q)

    log.info(
        f"Univers final : {len(universe_quotes)} actions "
        f"({len(movers_set & set(universe_tickers))} movers, "
        f"{len(momentum_set & set(universe_tickers))} momentum, "
        f"{len(wl_set & set(universe_tickers))} watchlist)"
    )
    return universe_quotes, histories


def _format_money(v: any) -> str:
    if v is None or pd.isna(v):
        return "-"
    if v >= 1e12:
        return f"{v/1e12:.1f}T$"
    if v >= 1e9:
        return f"{v/1e9:.1f}B$"
    if v >= 1e6:
        return f"{v/1e6:.1f}M$"
    return f"{v:.0f}$"


def _format_vol(v: float) -> str:
    if v >= 1e9:
        return f"{v/1e9:.1f}B"
    if v >= 1e6:
        return f"{v/1e6:.1f}M"
    if v >= 1e3:
        return f"{v/1e3:.0f}K"
    return str(int(v))


# ---------------------------------------------------------------------------
# Enhanced stock analysis (cœur du worker)
# ---------------------------------------------------------------------------


def _fetch_history(ticker: str, period: str = "3mo") -> pd.DataFrame:
    try:
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        # yfinance 1.x returns MultiIndex columns even for single tickers;
        # flatten so hist["Close"] is always a Series, not a sub-DataFrame.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return pd.DataFrame()


def _fetch_earnings_flag(ticker: str) -> str:
    try:
        cal = yf.Ticker(ticker).calendar
        if cal is None:
            return "-"
        # calendar peut être un dict ou DataFrame selon la version
        if isinstance(cal, dict):
            date_val = cal.get("Earnings Date")
            if isinstance(date_val, list) and date_val:
                date_val = date_val[0]
        elif isinstance(cal, pd.DataFrame) and "Earnings Date" in cal.columns:
            date_val = cal["Earnings Date"].iloc[0]
        else:
            return "-"
        if date_val is None:
            return "-"
        earnings_date = pd.Timestamp(date_val).date()
        days = (earnings_date - datetime.today().date()).days
        if 0 <= days <= 3:
            return f"⚠️ J+{days}"
        if 4 <= days <= 7:
            return f"📅 J+{days}"
        return "-"
    except Exception:
        return "-"

def _score_stock(quote: dict, hist: pd.DataFrame, spy_hist: pd.DataFrame, sector_scores: dict = None) -> dict | None:
    """
    Score par blocs indépendants avec plafonds pour éviter le double comptage.

    Bloc Tendance  (max 3.0) : MA50, MA200, R², pente
    Bloc Momentum  (max 2.5) : variation jour (avec confirmation volume/structure), MACD, RSI
    Bloc Strength  (max 2.0) : sommet 52s, RS vs SPY 1m+3m
    Bloc Risque    (ajust.)  : earnings, RSI contextuel, Bollinger extension
    """
    ticker = str(quote.get("symbol", "")).upper()
    name = str(quote.get("shortName") or quote.get("longName") or ticker)
    source = quote.get("_source", "screen")

    # Defensive: flatten MultiIndex columns if still present
    if not hist.empty and isinstance(hist.columns, pd.MultiIndex):
        hist = hist.copy()
        hist.columns = hist.columns.get_level_values(0)

    def _num(key, *fallbacks):
        for k in (key, *fallbacks):
            v = quote.get(k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return float("nan")

    price = _num("regularMarketPrice", "intradayprice")
    day_change = _num("regularMarketChangePercent", "percentchange")
    market_cap = _num("marketCap", "intradaymarketcap")
    volume = _num("regularMarketVolume", "dayvolume")
    avg_vol = _num("averageDailyVolume3Month", "averageDailyVolume10Day")
    fifty_day = _num("fiftyDayAverage")
    two_hundred_day = _num("twoHundredDayAverage")
    week_high = _num("fiftyTwoWeekHigh")

    if pd.isna(price) or pd.isna(day_change):
        return None

    signals: list[str] = []
    has_hist = not hist.empty and "Close" in hist.columns and len(hist) >= 30
    closes = _to_series(hist["Close"]) if has_hist else pd.Series(dtype=float)
    has_hist = has_hist and len(closes) >= 30

    # ==================================================================
    # BLOC TENDANCE (max 3.0)
    # ==================================================================
    b_trend = 0.0

    if not pd.isna(fifty_day) and price > fifty_day:
        b_trend += 1.2
        signals.append("MA50+")
    if not pd.isna(fifty_day) and not pd.isna(two_hundred_day) and fifty_day > two_hundred_day:
        b_trend += 1.2
        signals.append("50>200")

    r2_val, slope_val = (None, None)
    if has_hist:
        r2_val, slope_val = calc_trend_quality(closes, period=30)
        if r2_val >= 0.85 and slope_val > 0:
            b_trend += 1.0
            signals.append(f"tendance R²={r2_val:.2f}")
        elif r2_val >= 0.65 and slope_val > 0:
            b_trend += 0.5
        elif slope_val is not None and slope_val < 0:
            b_trend -= 0.4

    b_trend = min(b_trend, 3.0)

    # ==================================================================
    # BLOC MOMENTUM (max 2.5)
    # Variation du jour récompensée uniquement si confirmée (volume + structure)
    # ==================================================================
    b_momentum = 0.0

    if day_change > 0:
        vol_ok = not pd.isna(avg_vol) and avg_vol > 0 and not pd.isna(volume) and volume >= avg_vol * 1.3
        trend_ok = not pd.isna(fifty_day) and price > fifty_day
        if vol_ok and trend_ok:
            b_momentum += min(day_change, 5.0) * 0.20   # max +1.0 si confirmé des deux côtés
            signals.append("move confirmé vol+structure")
        elif vol_ok or trend_ok:
            b_momentum += min(day_change, 5.0) * 0.10   # max +0.5 si confirmé d'un seul côté
        else:
            b_momentum += min(day_change, 3.0) * 0.03   # max +0.09 si non confirmé : presque rien

    rsi_val, macd_label, boll_raw = None, "-", "neutre"
    if has_hist:
        rsi_val = round(calc_rsi(closes), 1)
        if 45 <= rsi_val <= 65:
            b_momentum += 1.0
            signals.append(f"RSI sain ({rsi_val})")
        elif 65 < rsi_val <= 75:
            b_momentum += 0.4
            signals.append(f"RSI fort ({rsi_val})")
        # RSI > 75 géré dans le bloc risque ci-dessous

        macd_raw = calc_macd_signal(closes)
        macd_label = {
            "croisement_haussier": "🟢 Croisement↑",
            "haussier": "🟢 Haussier",
            "croisement_baissier": "🔴 Croisement↓",
            "baissier": "🔴 Baissier",
            "neutre": "🟡 Neutre",
        }.get(macd_raw, "🟡 Neutre")
        if macd_raw == "croisement_haussier":
            b_momentum += 1.5
        elif macd_raw == "haussier":
            b_momentum += 0.8
        elif macd_raw == "croisement_baissier":
            b_momentum -= 0.6
        elif macd_raw == "baissier":
            b_momentum -= 0.3

        boll_raw = calc_bollinger_position(closes)

    b_momentum = max(min(b_momentum, 2.5), -1.0)

    # ==================================================================
    # BLOC STRENGTH (max 2.0)
    # ==================================================================
    b_strength = 0.0
    rs_1m, rs_3m = None, None

    if not pd.isna(week_high) and week_high > 0:
        proximity = price / week_high
        if proximity >= 0.95:
            b_strength += 1.2
            signals.append("sommet 52s proche")
        elif proximity >= 0.90:
            b_strength += 0.8

    if has_hist and not spy_hist.empty and "Close" in spy_hist.columns:
        rs_1m = calc_relative_strength(closes, spy_hist["Close"], 21)
        rs_3m = calc_relative_strength(closes, spy_hist["Close"], 63)
        if rs_1m is not None and rs_3m is not None and rs_1m > 0 and rs_3m > 0:
            b_strength += 0.8
            signals.append("RS+ SPY 1m+3m")
        if rs_1m is not None and rs_1m > 5:
            b_strength += 0.4
        if rs_3m is not None and rs_3m > 10:
            b_strength += 0.3

    b_strength = min(b_strength, 2.0)

    # ==================================================================
    # BLOC RISQUE (ajustements négatifs)
    # ==================================================================
    b_risk = 0.0
    boll_label = {
        "extension": "↑ Extension",
        "zone_haussiere": "✅ Zone haute",
        "zone_baissiere": "⬇ Zone basse",
        "sous_bande": "⚠️ Sous bande",
        "neutre": "— Neutre",
    }.get(boll_raw, "—")

    if has_hist and rsi_val is not None and rsi_val > 75:
        # Contextuel : malus fort si le prix est aussi très éloigné de la MA20
        ma20 = float(closes.rolling(20).mean().iloc[-1]) if len(closes) >= 20 else float("nan")
        if not pd.isna(ma20) and ma20 > 0 and (price - ma20) / ma20 > 0.15:
            b_risk -= 1.2   # Extension ET RSI extrême → vrai signal de surachat
            signals.append(f"RSI suracheté+extension ({rsi_val})")
        else:
            b_risk -= 0.4   # RSI élevé mais pas encore en extension : malus léger
            signals.append(f"RSI élevé ({rsi_val})")

    if boll_raw == "extension":
        if rsi_val is not None and rsi_val > 70:
            b_risk -= 0.5   # Extension Bollinger + RSI haut : double signal surachat
        else:
            b_risk -= 0.2   # Extension seule : léger avertissement

    if boll_raw == "sous_bande":
        b_risk -= 0.3

    # earnings : géré séparément après (tag ⚠️ uniquement dans le top 15)

    # ==================================================================
    # BLOC SETUP QUALITY (max 2.0)
    # Répond à : "est-ce le bon MOMENT d'entrer dans cette tendance ?"
    # Les autres blocs mesurent la qualité de la tendance passée.
    # Celui-ci mesure si le prix est dans une position favorable pour entrer.
    # ==================================================================
    b_setup = 0.0
    setup_label = "-"

    if has_hist and len(closes) >= 20:
        ma20 = float(closes.rolling(20).mean().iloc[-1])
        hist_vol = _to_series(hist["Volume"]) if "Volume" in hist.columns else pd.Series(dtype=float)

        # --- Signal 1 : distance du prix à la MA20 ---
        # Idéal : action en tendance qui revient "respirer" sur sa MA20
        # C'est là qu'on entre, pas quand elle est à +25% au-dessus
        if ma20 > 0:
            dist_pct = (price - ma20) / ma20 * 100
            if 0 <= dist_pct < 3:
                b_setup += 1.2
                setup_label = "pullback MA20"
                signals.append("setup: pullback MA20 ✓")
            elif 3 <= dist_pct < 7:
                b_setup += 0.6
                setup_label = "léger écart MA20"
            elif 7 <= dist_pct < 15:
                b_setup += 0.0   # neutre
                setup_label = "écarté MA20"
            elif dist_pct >= 15:
                b_setup -= 0.6   # trop étiré, entrée risquée
                setup_label = "étiré MA20"
                signals.append("setup: trop loin MA20 ✗")
            elif dist_pct < 0:
                b_setup -= 0.3   # sous la MA20, tendance fragilisée
                setup_label = "sous MA20"

        # --- Signal 2 : tarissement du volume (volume dry-up) ---
        # Sur un pullback sain, le volume baisse (pas de distribution).
        # Si les vendeurs ne sont pas là, le rebond peut être rapide.
        if len(hist_vol) >= 20:
            vol_last3 = float(hist_vol.iloc[-3:].mean())
            vol_avg20 = float(hist_vol.rolling(20).mean().iloc[-1])
            if vol_avg20 > 0:
                vol_ratio = vol_last3 / vol_avg20
                if vol_ratio < 0.6:
                    b_setup += 0.8
                    signals.append("setup: volume tari ✓")
                elif vol_ratio < 0.8:
                    b_setup += 0.4
                elif vol_ratio > 1.8:
                    b_setup -= 0.3   # volume élevé sur baisse = distribution possible

        # --- Signal 3 : range serré sur 5 jours (base/consolidation) ---
        # Une action qui fait une "pause" en range étroit avant un move
        # est plus intéressante qu'une action qui part dans tous les sens
        if "High" in hist.columns and "Low" in hist.columns and len(hist) >= 20:
            h_high = _to_series(hist["High"])
            h_low = _to_series(hist["Low"])
            last5_range = float((h_high.iloc[-5:].max() - h_low.iloc[-5:].min()) / price * 100)
            atr20 = float((h_high - h_low).rolling(20).mean().iloc[-1] / price * 100)
            if atr20 > 0:
                compression = last5_range / (atr20 * 5)   # < 0.5 = range comprimé
                if compression < 0.4:
                    b_setup += 0.5
                    signals.append("setup: range comprimé ✓")
                elif compression < 0.6:
                    b_setup += 0.2

    b_setup = max(min(b_setup, 2.0), -1.0)

    # ==================================================================
    # BLOC SECTEUR (Optionnel - Bonus si secteur fort)
    # ==================================================================
    b_sector = 0.0
    # Si tu passes les scores de secteurs au worker, tu peux ajouter un bonus ici
    # if sector_scores and quote.get('sector') in sector_scores:
    #    if sector_scores[quote['sector']] > 0: b_sector = 0.5

    # ==================================================================
    # Score final — 4 blocs + risque
    # Max théorique : 3.0 + 2.5 + 2.0 + 2.0 = 9.5 (plafonné à 10)
    # ==================================================================
    raw = b_trend + b_momentum + b_strength + b_setup + b_risk
    score_final = round(min(max(raw, 0.0), 10.0), 1)

    return {
        "Nom": name,
        "Ticker": ticker,
        "Source": source,
        "Cours": round(price, 2),
        "Variation (%)": round(day_change, 2),
        "Capitalisation": _format_money(market_cap) if not pd.isna(market_cap) else "-",
        "cap_raw": market_cap if not pd.isna(market_cap) else 0.0,
        "Volume": _format_vol(volume) if not pd.isna(volume) else "-",
        "Score": score_final,
        "B_Tendance": round(b_trend, 1),
        "B_Momentum": round(b_momentum, 1),
        "B_Force": round(b_strength, 1),
        "B_Setup": round(b_setup, 1),
        "B_Risque": round(b_risk, 1),
        "Setup": setup_label,
        "RSI": rsi_val,
        "MACD": macd_label,
        "Bollinger": boll_label,
        "R2_tendance": r2_val,
        "RS_SPY_1m (%)": rs_1m,
        "RS_SPY_3m (%)": rs_3m,
        "Signaux": ", ".join(signals[:5]) or "-",
    }


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


def job_refresh_market() -> None:
    log.info("job_refresh_market — debut")
    try:
        # Movers : gainers / losers
        gainers_q = yf.EquityQuery("and", [
            yf.EquityQuery("eq", ["region", "us"]),
            yf.EquityQuery("is-in", ["exchange", "NMS", "NYQ"]),
            yf.EquityQuery("gte", ["intradaymarketcap", 5_000_000_000]),
            yf.EquityQuery("gt", ["dayvolume", 500_000]),
        ])
        payload = yf.screen(gainers_q, size=50, sortField="percentchange", sortAsc=False)
        quotes = payload.get("quotes", [])
        gainers = [
            {
                "ticker": q.get("symbol"),
                "name": q.get("shortName") or q.get("symbol"),
                "change": round(float(q.get("regularMarketChangePercent") or 0), 2),
                "price": round(float(q.get("regularMarketPrice") or 0), 2),
            }
            for q in quotes[:10] if q.get("symbol")
        ]
        losers = sorted(gainers, key=lambda x: x["change"])[:5]
        gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:5]
        write_cache("movers", {"gainers": gainers, "losers": losers})
        log.info(f"job_refresh_market — {len(gainers)} gainers, {len(losers)} losers")
    except Exception as e:
        log.error(f"job_refresh_market error: {e}")


def job_score_stocks() -> None:
    log.info("job_score_stocks — debut")
    try:
        spy_hist = _fetch_history("SPY", "3mo")
        universe_quotes, histories = build_candidate_universe(spy_hist)

        if not universe_quotes:
            log.warning("job_score_stocks — univers vide")
            return

        results = []
        for quote in universe_quotes:
            ticker = str(quote.get("symbol", "")).upper()
            hist = histories.get(ticker, pd.DataFrame())
            row = _score_stock(quote, hist, spy_hist)
            if row:
                results.append(row)

        results.sort(key=lambda r: r["Score"], reverse=True)

        # Earnings flags sur tout l'univers — aucune action affichée ne sera sans warning
        with ThreadPoolExecutor(max_workers=4) as pool:
            future_map = {pool.submit(_fetch_earnings_flag, r["Ticker"]): i for i, r in enumerate(results)}
            for fut in as_completed(future_map):
                idx = future_map[fut]
                try:
                    flag = fut.result()
                    results[idx]["Earnings"] = flag
                    if "⚠️" in flag:
                        results[idx]["B_Risque"] = round(results[idx]["B_Risque"] - 0.8, 1)
                        results[idx]["Score"] = round(max(results[idx]["Score"] - 0.8, 0), 1)
                except Exception:
                    results[idx]["Earnings"] = "-"

        results.sort(key=lambda r: r["Score"], reverse=True)
        write_cache("stock_ideas", results)

        top3 = " | ".join(f"{r['Ticker']} ({r['Score']})" for r in results[:3])
        log.info(f"job_score_stocks — {len(results)} actions scorees. Top 3 : {top3}")
    except Exception as e:
        log.error(f"job_score_stocks error: {e}", exc_info=True)


def job_score_sectors() -> None:
    log.info("job_score_sectors — debut")
    try:
        tickers = list(SECTOR_ETFS.values()) + ["SPY"]
        hists: dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            future_map = {pool.submit(_fetch_history, t, "1mo"): t for t in tickers}
            for fut in as_completed(future_map):
                t = future_map[fut]
                try:
                    hists[t] = fut.result()
                except Exception:
                    hists[t] = pd.DataFrame()

        spy_hist = hists.get("SPY", pd.DataFrame())
        rows = []
        for name, etf in SECTOR_ETFS.items():
            h = hists.get(etf, pd.DataFrame())
            if h.empty or "Close" not in h.columns or len(h) < 5:
                continue
            closes = _to_series(h["Close"])
            if len(closes) < 5:
                continue
            spy_closes = _to_series(spy_hist["Close"]) if not spy_hist.empty and "Close" in spy_hist.columns else closes
            perf_1m = round((float(closes.iloc[-1]) / float(closes.iloc[0]) - 1) * 100, 2)
            rs = calc_relative_strength(closes, spy_closes, 21)
            rows.append({"Secteur": name, "ETF": etf, "Perf 1m (%)": perf_1m, "RS vs SPY (%)": rs or 0.0})

        rows.sort(key=lambda r: r["Perf 1m (%)"], reverse=True)
        write_cache("sectors", rows)
        log.info(f"job_score_sectors — {len(rows)} secteurs scores")
    except Exception as e:
        log.error(f"job_score_sectors error: {e}")


def job_morning_briefing() -> None:
    log.info("job_morning_briefing — debut")
    try:
        # Pré-calcul du contexte marché du matin
        today = datetime.now().strftime("%Y-%m-%d")
        tickers_index = ["SPY", "QQQ", "DIA", "^VIX", "GLD", "BTC-USD"]
        snapshot = {}
        for t in tickers_index:
            try:
                hist = yf.download(t, period="5d", progress=False, auto_adjust=True)
                if isinstance(hist.columns, pd.MultiIndex):
                    hist.columns = hist.columns.get_level_values(0)
                if not hist.empty and "Close" in hist.columns:
                    price = float(hist["Close"].iloc[-1])
                    prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else price
                    change = round((price / prev - 1) * 100, 2)
                    snapshot[t] = {"price": round(price, 2), "change": change}
            except Exception:
                pass
        write_cache("morning_snapshot", {"date": today, "indices": snapshot})
        log.info(f"job_morning_briefing — snapshot de {len(snapshot)} indices")
    except Exception as e:
        log.error(f"job_morning_briefing error: {e}")


def job_evening_snapshot() -> None:
    log.info("job_evening_snapshot — debut")
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        cached_ideas = read_cache("stock_ideas")
        data = {
            "date": today,
            "stock_ideas_count": len(cached_ideas["data"]) if cached_ideas else 0,
            "top3": (cached_ideas["data"][:3] if cached_ideas and cached_ideas.get("data") else []),
        }
        write_cache(f"snapshot_{today}", data)
        log.info(f"job_evening_snapshot — snapshot {today} sauvegarde")
    except Exception as e:
        log.error(f"job_evening_snapshot error: {e}")


def job_weekend_maintenance() -> None:
    log.info("job_weekend_maintenance — debut")
    try:
        cache_dir = BASE_DIR / "data" / "cache"
        cutoff = datetime.now() - timedelta(days=90)
        removed = 0
        for f in cache_dir.glob("snapshot_*.json"):
            try:
                date_str = f.stem.replace("snapshot_", "")
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if file_date < cutoff:
                    f.unlink()
                    removed += 1
            except Exception:
                pass
        log.info(f"job_weekend_maintenance — {removed} snapshots supprimes")

        # Rotation du log worker si > 10 MB
        log_path = BASE_DIR / "data" / "worker.log"
        if log_path.exists() and log_path.stat().st_size > 10 * 1024 * 1024:
            log_path.rename(log_path.with_suffix(".log.old"))
            log.info("worker.log rotate")
    except Exception as e:
        log.error(f"job_weekend_maintenance error: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Worker demarrage — 4 cores / 8 GB")

    # Premier calcul immédiat au démarrage
    log.info("Calculs initiaux...")
    job_refresh_market()
    job_score_stocks()
    job_score_sectors()
    job_morning_briefing()
    log.info("Calculs initiaux termines — scheduler en route")

    scheduler = BlockingScheduler(
        executors={"default": APSThreadPool(4)},
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 60},
    )

    scheduler.add_job(job_refresh_market, "interval", minutes=5, id="refresh_market")
    scheduler.add_job(job_score_stocks, "interval", minutes=30, id="score_stocks")
    scheduler.add_job(job_score_sectors, "interval", minutes=60, id="score_sectors")
    scheduler.add_job(job_morning_briefing, "cron", hour=7, minute=0, id="morning_briefing")
    scheduler.add_job(job_evening_snapshot, "cron", hour=18, minute=0, id="evening_snapshot")
    scheduler.add_job(
        job_weekend_maintenance, "cron", day_of_week="sun", hour=2, minute=0, id="maintenance"
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Worker arrete proprement.")
