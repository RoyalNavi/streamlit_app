from __future__ import annotations

import logging
import os
from datetime import datetime

import pandas as pd
import yfinance as yf


log = logging.getLogger("market_regime")

MARKET_REGIME_ENABLED = os.getenv("MARKET_REGIME_ENABLED", "1").lower() not in {"0", "false", "no"}
RISK_ON = "RISK_ON"
NEUTRAL = "NEUTRAL"
RISK_OFF = "RISK_OFF"


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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


def _fetch_histories(tickers: list[str], period: str = "1y") -> dict[str, pd.DataFrame]:
    histories = {ticker: pd.DataFrame() for ticker in tickers}
    try:
        raw = yf.download(
            " ".join(tickers),
            period=period,
            progress=False,
            auto_adjust=True,
            group_by="ticker",
            threads=False,
        )
        for ticker in tickers:
            histories[ticker] = _normalize_history_frame(raw, ticker)
    except Exception as exc:
        log.warning("market regime history batch failed: %s", exc)
    return histories


def _close_series(hist: pd.DataFrame) -> pd.Series:
    if hist.empty or "Close" not in hist.columns:
        return pd.Series(dtype=float)
    return hist["Close"].astype(float).dropna()


def _asset_metrics(hist: pd.DataFrame) -> dict:
    closes = _close_series(hist)
    if len(closes) < 60:
        return {}
    price = float(closes.iloc[-1])
    ma50 = float(closes.tail(50).mean())
    ma200 = float(closes.tail(min(200, len(closes))).mean())
    prev_ma50 = float(closes.iloc[-60:-10].mean()) if len(closes) >= 60 else ma50
    high20 = float(closes.tail(20).max())
    return {
        "price": round(price, 4),
        "ma50": round(ma50, 4),
        "ma200": round(ma200, 4),
        "above_ma50": price > ma50,
        "above_ma200": price > ma200,
        "ma50_slope_pct": round((ma50 / prev_ma50 - 1) * 100, 2) if prev_ma50 else 0.0,
        "drawdown_20d_pct": round((price / high20 - 1) * 100, 2) if high20 else 0.0,
    }


def compute_market_regime() -> dict:
    if not MARKET_REGIME_ENABLED:
        return {"regime": NEUTRAL, "enabled": False, "calculated_at": utc_now_iso(), "score": 0, "reasons": ["disabled"]}

    histories = _fetch_histories(["SPY", "QQQ", "IWM"], "1y")
    metrics = {ticker: _asset_metrics(histories.get(ticker, pd.DataFrame())) for ticker in ["SPY", "QQQ", "IWM"]}
    spy = metrics.get("SPY") or {}
    qqq = metrics.get("QQQ") or {}
    iwm = metrics.get("IWM") or {}

    if not spy or not qqq:
        return {"regime": NEUTRAL, "enabled": True, "calculated_at": utc_now_iso(), "score": 0, "reasons": ["insufficient data"], "metrics": metrics}

    score = 0
    reasons = []
    for ticker, item in (("SPY", spy), ("QQQ", qqq)):
        if item.get("above_ma50"):
            score += 1
            reasons.append(f"{ticker}>MA50")
        else:
            score -= 1
            reasons.append(f"{ticker}<MA50")
        if item.get("above_ma200"):
            score += 1
            reasons.append(f"{ticker}>MA200")
        else:
            score -= 1
            reasons.append(f"{ticker}<MA200")
        if float(item.get("ma50_slope_pct") or 0) > 0:
            score += 1
            reasons.append(f"{ticker} MA50+")
        if float(item.get("drawdown_20d_pct") or 0) <= -5:
            score -= 1
            reasons.append(f"{ticker} drawdown20")

    iwm_closes = _close_series(histories.get("IWM", pd.DataFrame()))
    spy_closes = _close_series(histories.get("SPY", pd.DataFrame()))
    iwm_rs_20d = None
    if len(iwm_closes) >= 21 and len(spy_closes) >= 21:
        iwm_perf = float(iwm_closes.iloc[-1] / iwm_closes.iloc[-21] - 1)
        spy_perf = float(spy_closes.iloc[-1] / spy_closes.iloc[-21] - 1)
        iwm_rs_20d = round((iwm_perf - spy_perf) * 100, 2)
        if iwm_rs_20d > 1:
            score += 1
            reasons.append("IWM>SPY")
        elif iwm_rs_20d < -2:
            score -= 1
            reasons.append("IWM<SPY")

    if score >= 5:
        regime = RISK_ON
    elif score <= 1 or (not spy.get("above_ma200") and not qqq.get("above_ma200")):
        regime = RISK_OFF
    else:
        regime = NEUTRAL

    return {
        "regime": regime,
        "enabled": True,
        "calculated_at": utc_now_iso(),
        "score": score,
        "reasons": reasons[:8],
        "metrics": metrics,
        "iwm_rs_20d_pct": iwm_rs_20d,
    }


def market_regime_adjustment(row: dict, engine: str, regime: str) -> float:
    setup = str(row.get("Setup_Type") or row.get("setup") or "").lower()
    is_breakout = "breakout" in setup
    if regime == RISK_OFF:
        if engine == "smallcap":
            return -1.5 if is_breakout else -1.0
        return -0.7 if is_breakout else 0.0
    if regime == NEUTRAL:
        if engine == "smallcap":
            return -0.5 if is_breakout else -0.35
        return -0.25 if is_breakout else 0.0
    return 0.0


def apply_market_regime_adjustment(rows: list[dict], engine: str, regime_payload: dict) -> dict:
    regime = regime_payload.get("regime") or NEUTRAL
    adjusted = 0
    total_penalty = 0.0
    score_key = "Explosion_Score" if engine == "smallcap" else "Score"
    for row in rows:
        adjustment = market_regime_adjustment(row, engine, regime)
        row["market_regime"] = regime
        row["market_regime_adjustment"] = round(adjustment, 2)
        if adjustment:
            current = float(row.get(score_key) or 0)
            row[score_key] = round(max(current + adjustment, 0.0), 1)
            adjusted += 1
            total_penalty += adjustment
    return {"regime": regime, "adjusted": adjusted, "total_adjustment": round(total_penalty, 2)}
