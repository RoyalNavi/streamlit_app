from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yfinance as yf


BASE_DIR = Path(__file__).resolve().parent
SIGNALS_DB_PATH = BASE_DIR / "data" / "signals.sqlite3"
TRACKING_HORIZONS = (1, 3, 5)
MAX_TRACKED_SIGNALS_PER_ENGINE_DAY = 10
SIGNAL_LAB_STATUSES = ("A surveiller", "Entre", "Ignore", "Sorti", "Invalide")

log = logging.getLogger("signal_tracking")


FetchHistoriesFn = Callable[[list[str], str], dict[str, pd.DataFrame]]


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "").replace("+00:00", ""))
    except Exception:
        return None


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _json_loads(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _normalize_history_frame(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    result = df.copy()
    if isinstance(result.columns, pd.MultiIndex):
        target = str(ticker or "").upper()
        matched_target = False
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
                        matched_target = True
                        break
                    except Exception:
                        pass
            if not matched_target:
                return pd.DataFrame()
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


def _default_fetch_histories(tickers: list[str], period: str = "2mo") -> dict[str, pd.DataFrame]:
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
            log.warning("tracking history batch failed: %s", exc)
        for ticker in chunk:
            if histories.get(ticker, pd.DataFrame()).empty:
                try:
                    raw_one = yf.download(ticker, period=period, progress=False, auto_adjust=True, threads=False)
                    histories[ticker] = _normalize_history_frame(raw_one, ticker)
                except Exception:
                    histories[ticker] = pd.DataFrame()
    return histories


def init_tracking_db(db_path: Path = SIGNALS_DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_signals (
                signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                engine TEXT NOT NULL,
                ticker TEXT NOT NULL,
                name TEXT,
                detected_at TEXT NOT NULL,
                observation_key TEXT NOT NULL,
                reference_price REAL NOT NULL,
                score REAL,
                setup TEXT,
                signal_quality TEXT,
                tags_json TEXT,
                risk TEXT,
                run_id TEXT,
                rank INTEGER,
                market_timestamp TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(engine, ticker, observation_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                signal_id INTEGER PRIMARY KEY,
                engine TEXT NOT NULL,
                ticker TEXT NOT NULL,
                perf_1d_pct REAL,
                perf_3d_pct REAL,
                perf_5d_pct REAL,
                max_runup_pct REAL,
                max_drawdown_pct REAL,
                days_to_peak INTEGER,
                days_to_trough INTEGER,
                last_followup_timestamp TEXT,
                followup_complete INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT,
                FOREIGN KEY(signal_id) REFERENCES tracked_signals(signal_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tracked_signals_engine_time ON tracked_signals(engine, detected_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tracked_signals_ticker_time ON tracked_signals(ticker, detected_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_outcomes_complete ON signal_outcomes(followup_complete)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_lab_decisions (
                decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_key TEXT NOT NULL,
                engine TEXT NOT NULL,
                ticker TEXT NOT NULL,
                name TEXT,
                status TEXT NOT NULL,
                decision_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source_detected_at TEXT NOT NULL,
                reference_price REAL,
                score REAL,
                setup TEXT,
                signal_quality TEXT,
                risk TEXT,
                source_rank INTEGER,
                notes TEXT,
                metadata_json TEXT,
                UNIQUE(user_key, engine, ticker, source_detected_at)
            )
            """
        )
        _ensure_column(conn, "signal_lab_decisions", "source_rank", "INTEGER")
        _ensure_column(conn, "signal_lab_decisions", "notes", "TEXT")
        _ensure_column(conn, "signal_lab_decisions", "metadata_json", "TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_lab_user_status ON signal_lab_decisions(user_key, status, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_lab_signal ON signal_lab_decisions(engine, ticker, source_detected_at)")


def _observation_key(engine: str, ticker: str, detected_at: str, market_timestamp: str | None) -> str:
    parsed = _parse_iso(detected_at) or _parse_iso(market_timestamp) or datetime.utcnow()
    # Une observation par moteur/ticker/jour de recommandation. Cela garde le suivi lisible cote utilisateur.
    return f"{engine}:{ticker}:{parsed.date().isoformat()}"


def _standard_signal_payload(row: dict, run_id: str | None, detected_at: str) -> dict | None:
    ticker = str(row.get("Ticker") or "").upper()
    reference_price = _to_float(row.get("last_observed_price") or row.get("Cours"))
    if not ticker or reference_price is None or reference_price <= 0:
        return None
    market_timestamp = row.get("last_market_timestamp")
    return {
        "engine": "standard",
        "ticker": ticker,
        "name": row.get("Nom") or ticker,
        "detected_at": detected_at,
        "observation_key": _observation_key("standard", ticker, detected_at, market_timestamp),
        "reference_price": reference_price,
        "score": _to_float(row.get("Score")),
        "setup": row.get("Setup_Type") or row.get("Setup"),
        "signal_quality": "confirmed" if row.get("Confirmed") else "candidate",
        "tags": row.get("why_selected") or [],
        "risk": ", ".join(row.get("risk_flags") or []) if isinstance(row.get("risk_flags"), list) else row.get("risk_flags"),
        "run_id": run_id,
        "rank": int(_to_float(row.get("Display_Rank") or row.get("Rank_Global"), 0) or 0),
        "market_timestamp": market_timestamp,
        "metadata": {
            "raw_score": row.get("Raw_Score"),
            "confirmed": bool(row.get("Confirmed")),
            "stability_score": row.get("Stability_Score"),
            "consecutive_hits": row.get("Consecutive_Hits"),
            "new_observation": bool(row.get("new_observation")),
            "source": row.get("Source"),
            "market_region": row.get("market_region"),
        },
    }


def _smallcap_signal_payload(row: dict, run_id: str | None, detected_at: str) -> dict | None:
    ticker = str(row.get("ticker") or "").upper()
    reference_price = _to_float(row.get("last_observed_price") or row.get("price"))
    if not ticker or reference_price is None or reference_price <= 0:
        return None
    market_timestamp = row.get("last_market_timestamp")
    return {
        "engine": "smallcap",
        "ticker": ticker,
        "name": row.get("name") or ticker,
        "detected_at": detected_at,
        "observation_key": _observation_key("smallcap", ticker, detected_at, market_timestamp),
        "reference_price": reference_price,
        "score": _to_float(row.get("Explosion_Score")),
        "setup": row.get("setup"),
        "signal_quality": row.get("signal_quality"),
        "tags": row.get("tags") or [],
        "risk": row.get("risk"),
        "run_id": run_id,
        "rank": int(_to_float(row.get("rank"), 0) or 0),
        "market_timestamp": market_timestamp,
        "metadata": {
            "change_pct": row.get("change_pct"),
            "rel_volume": row.get("rel_volume"),
            "market_cap": row.get("market_cap"),
            "comment": row.get("comment"),
            "sources": row.get("sources"),
        },
    }


def register_detected_signals(
    engine: str,
    rows: list[dict],
    *,
    run_id: str | None = None,
    detected_at: str | None = None,
    db_path: Path = SIGNALS_DB_PATH,
    max_per_day: int = MAX_TRACKED_SIGNALS_PER_ENGINE_DAY,
) -> dict[str, int]:
    init_tracking_db(db_path)
    detected_at = detected_at or utc_now_iso()
    inserted = 0
    skipped = 0
    capped = 0
    payloads = []
    for row in rows:
        if engine == "standard":
            payload = _standard_signal_payload(row, run_id, detected_at)
        elif engine == "smallcap":
            payload = _smallcap_signal_payload(row, run_id, detected_at)
        else:
            raise ValueError(f"Unknown signal engine: {engine}")
        if payload:
            payloads.append(payload)

    payloads.sort(
        key=lambda payload: (
            int(payload.get("rank") or 999_999),
            -float(payload.get("score") or 0),
            str(payload.get("ticker") or ""),
        )
    )

    with sqlite3.connect(db_path) as conn:
        for payload in payloads:
            detected_day = str(payload["detected_at"])[:10]
            tracked_today = conn.execute(
                """
                SELECT COUNT(*)
                FROM tracked_signals
                WHERE engine = ? AND substr(detected_at, 1, 10) = ?
                """,
                (payload["engine"], detected_day),
            ).fetchone()[0]
            if max_per_day > 0 and tracked_today >= max_per_day:
                capped += 1
                continue
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO tracked_signals (
                    engine, ticker, name, detected_at, observation_key, reference_price,
                    score, setup, signal_quality, tags_json, risk, run_id, rank,
                    market_timestamp, metadata_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["engine"],
                    payload["ticker"],
                    payload["name"],
                    payload["detected_at"],
                    payload["observation_key"],
                    payload["reference_price"],
                    payload["score"],
                    payload["setup"],
                    payload["signal_quality"],
                    _json_dumps(payload.get("tags") or []),
                    payload["risk"],
                    payload["run_id"],
                    payload["rank"],
                    payload["market_timestamp"],
                    _json_dumps(payload.get("metadata") or {}),
                    utc_now_iso(),
                ),
            )
            if cur.rowcount:
                inserted += 1
                signal_id = cur.lastrowid
                conn.execute(
                    """
                    INSERT OR IGNORE INTO signal_outcomes (signal_id, engine, ticker, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (signal_id, payload["engine"], payload["ticker"], utc_now_iso()),
                )
            else:
                skipped += 1
    return {"inserted": inserted, "skipped": skipped, "capped": capped, "seen": len(payloads)}


def _daily_frame_after_signal(hist: pd.DataFrame, signal_at: str) -> pd.DataFrame:
    if hist.empty or "Close" not in hist.columns:
        return pd.DataFrame()
    frame = hist.copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame[frame.index.notna()].sort_index()
    detected = _parse_iso(signal_at)
    if detected is None:
        return pd.DataFrame()
    detected_day = pd.Timestamp(detected.date())
    return frame[frame.index.normalize() > detected_day].copy()


def _performance_from_history(signal: sqlite3.Row, hist: pd.DataFrame) -> dict:
    reference = _to_float(signal["reference_price"])
    if reference is None or reference <= 0:
        return {}
    future = _daily_frame_after_signal(hist, signal["detected_at"])
    if future.empty:
        return {}

    closes = future["Close"].dropna().astype(float)
    highs = future["High"].dropna().astype(float) if "High" in future.columns else closes
    lows = future["Low"].dropna().astype(float) if "Low" in future.columns else closes
    if closes.empty:
        return {}

    result: dict[str, Any] = {}
    for horizon in TRACKING_HORIZONS:
        if len(closes) >= horizon:
            result[f"perf_{horizon}d_pct"] = round((float(closes.iloc[horizon - 1]) / reference - 1) * 100, 2)

    window_size = min(len(future), max(TRACKING_HORIZONS))
    run_window = future.iloc[:window_size]
    if not run_window.empty:
        run_highs = run_window["High"].dropna().astype(float) if "High" in run_window.columns else run_window["Close"].dropna().astype(float)
        run_lows = run_window["Low"].dropna().astype(float) if "Low" in run_window.columns else run_window["Close"].dropna().astype(float)
        if not run_highs.empty:
            peak_pos = int(run_highs.reset_index(drop=True).idxmax())
            result["max_runup_pct"] = round((float(run_highs.iloc[peak_pos]) / reference - 1) * 100, 2)
            result["days_to_peak"] = peak_pos + 1
        if not run_lows.empty:
            trough_pos = int(run_lows.reset_index(drop=True).idxmin())
            result["max_drawdown_pct"] = round((float(run_lows.iloc[trough_pos]) / reference - 1) * 100, 2)
            result["days_to_trough"] = trough_pos + 1

    result["last_followup_timestamp"] = closes.index[min(len(closes), max(TRACKING_HORIZONS)) - 1].isoformat()
    result["followup_complete"] = int(len(closes) >= max(TRACKING_HORIZONS))
    return result


def update_signal_outcomes(
    *,
    db_path: Path = SIGNALS_DB_PATH,
    fetch_histories_fn: FetchHistoriesFn | None = None,
    limit: int = 300,
) -> dict[str, int]:
    init_tracking_db(db_path)
    fetch_histories = fetch_histories_fn or _default_fetch_histories
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            WITH eligible_signals AS (
                SELECT signal_id
                FROM (
                    SELECT signal_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY engine, substr(detected_at, 1, 10)
                               ORDER BY COALESCE(rank, 999999), COALESCE(score, 0) DESC, signal_id
                           ) AS tracking_rank
                    FROM tracked_signals
                )
                WHERE tracking_rank <= ?
            )
            SELECT s.*, o.perf_1d_pct, o.perf_3d_pct, o.perf_5d_pct, o.followup_complete
            FROM tracked_signals s
            JOIN signal_outcomes o ON o.signal_id = s.signal_id
            JOIN eligible_signals e ON e.signal_id = s.signal_id
            WHERE COALESCE(o.followup_complete, 0) = 0
            ORDER BY s.detected_at ASC
            LIMIT ?
            """,
            (MAX_TRACKED_SIGNALS_PER_ENGINE_DAY, limit),
        ).fetchall()

    if not rows:
        return {"pending": 0, "updated": 0, "completed": 0}

    tickers = [row["ticker"] for row in rows]
    histories = fetch_histories(tickers, "2mo")
    updated = 0
    completed = 0
    with sqlite3.connect(db_path) as conn:
        for signal in rows:
            hist = histories.get(signal["ticker"], pd.DataFrame())
            perf = _performance_from_history(signal, hist)
            if not perf:
                continue
            conn.execute(
                """
                UPDATE signal_outcomes
                SET perf_1d_pct = COALESCE(?, perf_1d_pct),
                    perf_3d_pct = COALESCE(?, perf_3d_pct),
                    perf_5d_pct = COALESCE(?, perf_5d_pct),
                    max_runup_pct = COALESCE(?, max_runup_pct),
                    max_drawdown_pct = COALESCE(?, max_drawdown_pct),
                    days_to_peak = COALESCE(?, days_to_peak),
                    days_to_trough = COALESCE(?, days_to_trough),
                    last_followup_timestamp = COALESCE(?, last_followup_timestamp),
                    followup_complete = MAX(COALESCE(followup_complete, 0), ?),
                    updated_at = ?
                WHERE signal_id = ?
                """,
                (
                    perf.get("perf_1d_pct"),
                    perf.get("perf_3d_pct"),
                    perf.get("perf_5d_pct"),
                    perf.get("max_runup_pct"),
                    perf.get("max_drawdown_pct"),
                    perf.get("days_to_peak"),
                    perf.get("days_to_trough"),
                    perf.get("last_followup_timestamp"),
                    int(perf.get("followup_complete") or 0),
                    utc_now_iso(),
                    signal["signal_id"],
                ),
            )
            updated += 1
            completed += int(perf.get("followup_complete") or 0)
    return {"pending": len(rows), "updated": updated, "completed": completed}


def summarize_signal_outcomes(
    *,
    db_path: Path = SIGNALS_DB_PATH,
    since_days: int = 90,
) -> dict:
    init_tracking_db(db_path)
    cutoff = (datetime.utcnow() - timedelta(days=since_days)).replace(microsecond=0).isoformat() + "Z"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        total_rows = conn.execute(
            """
            WITH eligible_signals AS (
                SELECT signal_id
                FROM (
                    SELECT signal_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY engine, substr(detected_at, 1, 10)
                               ORDER BY COALESCE(rank, 999999), COALESCE(score, 0) DESC, signal_id
                           ) AS tracking_rank
                    FROM tracked_signals
                    WHERE detected_at >= ?
                )
                WHERE tracking_rank <= ?
            ),
            suspicious_reference_groups AS (
                SELECT s.engine,
                       substr(s.detected_at, 1, 10) AS detected_day,
                       ROUND(s.reference_price, 4) AS reference_key
                FROM tracked_signals s
                JOIN eligible_signals e ON e.signal_id = s.signal_id
                WHERE s.engine = 'standard'
                GROUP BY s.engine, detected_day, reference_key
                HAVING COUNT(DISTINCT s.ticker) >= 4
            )
            SELECT s.engine AS engine,
                   COUNT(*) AS total,
                   AVG(o.perf_1d_pct) AS avg_1d,
                   AVG(o.perf_3d_pct) AS avg_3d,
                   AVG(o.perf_5d_pct) AS avg_5d,
                   AVG(o.max_runup_pct) AS avg_runup,
                   AVG(o.max_drawdown_pct) AS avg_drawdown,
                   AVG(CASE WHEN o.perf_1d_pct > 0 THEN 1.0 WHEN o.perf_1d_pct IS NOT NULL THEN 0.0 END) AS win_1d,
                   AVG(CASE WHEN o.perf_3d_pct > 0 THEN 1.0 WHEN o.perf_3d_pct IS NOT NULL THEN 0.0 END) AS win_3d,
                   AVG(CASE WHEN o.perf_5d_pct > 0 THEN 1.0 WHEN o.perf_5d_pct IS NOT NULL THEN 0.0 END) AS win_5d,
                   SUM(CASE WHEN o.followup_complete THEN 1 ELSE 0 END) AS complete
            FROM tracked_signals s
            JOIN signal_outcomes o ON o.signal_id = s.signal_id
            JOIN eligible_signals e ON e.signal_id = s.signal_id
            LEFT JOIN suspicious_reference_groups g
              ON g.engine = s.engine
             AND g.detected_day = substr(s.detected_at, 1, 10)
             AND g.reference_key = ROUND(s.reference_price, 4)
            WHERE g.reference_key IS NULL
            GROUP BY s.engine
            ORDER BY s.engine
            """,
            (cutoff, MAX_TRACKED_SIGNALS_PER_ENGINE_DAY),
        ).fetchall()
        setup_rows = conn.execute(
            """
            WITH eligible_signals AS (
                SELECT signal_id
                FROM (
                    SELECT signal_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY engine, substr(detected_at, 1, 10)
                               ORDER BY COALESCE(rank, 999999), COALESCE(score, 0) DESC, signal_id
                           ) AS tracking_rank
                    FROM tracked_signals
                    WHERE detected_at >= ?
                )
                WHERE tracking_rank <= ?
            ),
            suspicious_reference_groups AS (
                SELECT s.engine,
                       substr(s.detected_at, 1, 10) AS detected_day,
                       ROUND(s.reference_price, 4) AS reference_key
                FROM tracked_signals s
                JOIN eligible_signals e ON e.signal_id = s.signal_id
                WHERE s.engine = 'standard'
                GROUP BY s.engine, detected_day, reference_key
                HAVING COUNT(DISTINCT s.ticker) >= 4
            )
            SELECT s.engine, COALESCE(s.setup, '-') AS setup, COUNT(*) AS total,
                   AVG(o.perf_5d_pct) AS avg_5d,
                   AVG(o.max_runup_pct) AS avg_runup,
                   AVG(CASE WHEN o.perf_5d_pct > 0 THEN 1.0 WHEN o.perf_5d_pct IS NOT NULL THEN 0.0 END) AS win_5d
            FROM tracked_signals s
            JOIN signal_outcomes o ON o.signal_id = s.signal_id
            JOIN eligible_signals e ON e.signal_id = s.signal_id
            LEFT JOIN suspicious_reference_groups g
              ON g.engine = s.engine
             AND g.detected_day = substr(s.detected_at, 1, 10)
             AND g.reference_key = ROUND(s.reference_price, 4)
            WHERE o.perf_5d_pct IS NOT NULL
              AND g.reference_key IS NULL
            GROUP BY s.engine, s.setup
            HAVING COUNT(*) >= 2
            ORDER BY avg_5d DESC
            LIMIT 10
            """,
            (cutoff, MAX_TRACKED_SIGNALS_PER_ENGINE_DAY),
        ).fetchall()
        excluded_row = conn.execute(
            """
            WITH eligible_signals AS (
                SELECT signal_id
                FROM (
                    SELECT signal_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY engine, substr(detected_at, 1, 10)
                               ORDER BY COALESCE(rank, 999999), COALESCE(score, 0) DESC, signal_id
                           ) AS tracking_rank
                    FROM tracked_signals
                    WHERE detected_at >= ?
                )
                WHERE tracking_rank <= ?
            ),
            suspicious_reference_groups AS (
                SELECT s.engine,
                       substr(s.detected_at, 1, 10) AS detected_day,
                       ROUND(s.reference_price, 4) AS reference_key
                FROM tracked_signals s
                JOIN eligible_signals e ON e.signal_id = s.signal_id
                WHERE s.engine = 'standard'
                GROUP BY s.engine, detected_day, reference_key
                HAVING COUNT(DISTINCT s.ticker) >= 4
            )
            SELECT COUNT(*)
            FROM tracked_signals s
            JOIN eligible_signals e ON e.signal_id = s.signal_id
            JOIN suspicious_reference_groups g
              ON g.engine = s.engine
             AND g.detected_day = substr(s.detected_at, 1, 10)
             AND g.reference_key = ROUND(s.reference_price, 4)
            """,
            (cutoff, MAX_TRACKED_SIGNALS_PER_ENGINE_DAY),
        ).fetchone()

    def clean(row: sqlite3.Row) -> dict:
        result = dict(row)
        for key, value in list(result.items()):
            if isinstance(value, float):
                result[key] = round(value, 2)
        for key in ("win_1d", "win_3d", "win_5d"):
            if key in result and result[key] is not None:
                result[key] = round(result[key] * 100, 1)
        return result

    return {
        "since_days": since_days,
        "by_engine": [clean(row) for row in total_rows],
        "top_setups": [clean(row) for row in setup_rows],
        "excluded_suspicious": int(excluded_row[0]) if excluded_row else 0,
    }


def save_signal_lab_decision(
    payload: dict,
    *,
    user_key: str = "default",
    status: str = "A surveiller",
    db_path: Path = SIGNALS_DB_PATH,
) -> dict[str, Any]:
    init_tracking_db(db_path)
    clean_status = status if status in SIGNAL_LAB_STATUSES else "A surveiller"
    now = utc_now_iso()
    source_detected_at = str(payload.get("source_detected_at") or payload.get("detected_at") or now)
    engine = str(payload.get("engine") or "").strip().lower()
    ticker = str(payload.get("ticker") or "").strip().upper()
    if not engine or not ticker:
        raise ValueError("engine and ticker are required")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO signal_lab_decisions (
                user_key, engine, ticker, name, status, decision_at, updated_at,
                source_detected_at, reference_price, score, setup, signal_quality,
                risk, source_rank, notes, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_key, engine, ticker, source_detected_at) DO UPDATE SET
                status = excluded.status,
                updated_at = excluded.updated_at,
                reference_price = COALESCE(excluded.reference_price, signal_lab_decisions.reference_price),
                score = COALESCE(excluded.score, signal_lab_decisions.score),
                setup = COALESCE(excluded.setup, signal_lab_decisions.setup),
                signal_quality = COALESCE(excluded.signal_quality, signal_lab_decisions.signal_quality),
                risk = COALESCE(excluded.risk, signal_lab_decisions.risk),
                source_rank = COALESCE(excluded.source_rank, signal_lab_decisions.source_rank),
                metadata_json = excluded.metadata_json
            """,
            (
                user_key,
                engine,
                ticker,
                payload.get("name") or ticker,
                clean_status,
                now,
                now,
                source_detected_at,
                _to_float(payload.get("reference_price")),
                _to_float(payload.get("score")),
                payload.get("setup"),
                payload.get("signal_quality"),
                payload.get("risk"),
                int(_to_float(payload.get("source_rank"), 0) or 0),
                payload.get("notes"),
                _json_dumps(payload.get("metadata") or {}),
            ),
        )
    return {"engine": engine, "ticker": ticker, "status": clean_status}


def update_signal_lab_decision_status(
    decision_id: int,
    status: str,
    *,
    user_key: str = "default",
    db_path: Path = SIGNALS_DB_PATH,
) -> bool:
    init_tracking_db(db_path)
    if status not in SIGNAL_LAB_STATUSES:
        raise ValueError(f"Unknown status: {status}")
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE signal_lab_decisions
            SET status = ?, updated_at = ?
            WHERE decision_id = ? AND user_key = ?
            """,
            (status, utc_now_iso(), decision_id, user_key),
        )
    return bool(cur.rowcount)


def list_signal_lab_decisions(
    *,
    user_key: str = "default",
    db_path: Path = SIGNALS_DB_PATH,
    since_days: int = 180,
) -> list[dict[str, Any]]:
    init_tracking_db(db_path)
    cutoff = (datetime.utcnow() - timedelta(days=since_days)).replace(microsecond=0).isoformat() + "Z"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT d.*,
                   o.perf_1d_pct,
                   o.perf_3d_pct,
                   o.perf_5d_pct,
                   o.max_runup_pct,
                   o.max_drawdown_pct,
                   o.followup_complete,
                   t.signal_id AS tracked_signal_id,
                   t.detected_at AS tracked_detected_at
            FROM signal_lab_decisions d
            LEFT JOIN tracked_signals t
              ON t.engine = d.engine
             AND t.ticker = d.ticker
             AND substr(t.detected_at, 1, 10) = substr(d.source_detected_at, 1, 10)
            LEFT JOIN signal_outcomes o ON o.signal_id = t.signal_id
            WHERE d.user_key = ? AND d.decision_at >= ?
            ORDER BY d.updated_at DESC, d.decision_id DESC
            """,
            (user_key, cutoff),
        ).fetchall()

    results: list[dict[str, Any]] = []
    seen_decisions: set[int] = set()
    for row in rows:
        item = dict(row)
        decision_id = int(item["decision_id"])
        if decision_id in seen_decisions:
            continue
        seen_decisions.add(decision_id)
        item["metadata"] = _json_loads(item.pop("metadata_json", None), {})
        results.append(item)
    return results


def summarize_signal_lab_decisions(
    *,
    user_key: str = "default",
    db_path: Path = SIGNALS_DB_PATH,
    since_days: int = 180,
) -> dict[str, Any]:
    rows = list_signal_lab_decisions(user_key=user_key, db_path=db_path, since_days=since_days)
    if not rows:
        return {"total": 0, "by_status": [], "by_engine": []}
    frame = pd.DataFrame(rows)
    by_status = (
        frame.groupby("status", dropna=False)
        .agg(
            total=("decision_id", "count"),
            avg_5d=("perf_5d_pct", "mean"),
            win_5d=("perf_5d_pct", lambda values: (values.dropna() > 0).mean() * 100 if values.notna().any() else None),
        )
        .reset_index()
    )
    by_engine = (
        frame.groupby("engine", dropna=False)
        .agg(
            total=("decision_id", "count"),
            active=("status", lambda values: int(values.isin(["A surveiller", "Entre"]).sum())),
            avg_5d=("perf_5d_pct", "mean"),
        )
        .reset_index()
    )

    def records(df: pd.DataFrame) -> list[dict[str, Any]]:
        output = []
        for item in df.to_dict("records"):
            for key, value in list(item.items()):
                if isinstance(value, float) and not pd.isna(value):
                    item[key] = round(value, 2)
                elif pd.isna(value):
                    item[key] = None
            output.append(item)
        return output

    return {
        "total": len(rows),
        "active": int(frame["status"].isin(["A surveiller", "Entre"]).sum()),
        "by_status": records(by_status),
        "by_engine": records(by_engine),
    }
