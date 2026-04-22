import base64
import json
import os
import smtplib
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from email.message import EmailMessage
import hashlib
import logging
import hmac
import shutil
import subprocess
from html import escape, unescape
from io import StringIO
from pathlib import Path
import re
import secrets as pysecrets
import sqlite3
import xml.etree.ElementTree as ET

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("rafik_app")

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from rafik_dashboard.config import load_local_env_file
from rafik_dashboard.pages.change import render_change_rates_section
from rafik_dashboard.ui.components import render_section_heading, render_summary_strip
from rafik_dashboard.ui.styles import inject_global_styles
from market_universe import (
    europe_equities_frame,
    infer_currency,
    infer_market_region,
    infer_market_session,
)

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - optional dependency at runtime
    yf = None

try:
    import trafilatura as _trafilatura
except ImportError:  # pragma: no cover - optional dependency at runtime
    _trafilatura = None


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
BRIEFINGS_DIR = DATA_DIR / "briefings"
MARKET_CACHE_PATH = DATA_DIR / "market_directory.csv"
CRYPTO_CACHE_PATH = DATA_DIR / "crypto_directory.csv"
MARKET_CACHE_TTL_SECONDS = 2 * 24 * 60 * 60
CRYPTO_CACHE_TTL_SECONDS = 12 * 60 * 60
USER_AGENT = "rafik-streamlit-app/1.0 (finance dashboard)"
MAX_COMPARISON_COUNT = 10
USER_DB_PATH = DATA_DIR / "users.sqlite3"
JWT_SECRET_PATH = DATA_DIR / "jwt_secret.key"
BENCHMARK_TICKERS = {
    "CAC 40": "^FCHI",
    "S&P 500": "^GSPC",
    "MSCI World": "URTH",
}
MARKET_BRIEFING_ASSETS = [
    {"ticker": "^FCHI", "name": "CAC 40", "group": "Indices"},
    {"ticker": "^GSPC", "name": "S&P 500", "group": "Indices"},
    {"ticker": "^IXIC", "name": "Nasdaq", "group": "Indices"},
    {"ticker": "URTH", "name": "MSCI World", "group": "Indices"},
    {"ticker": "^VIX", "name": "VIX", "group": "Risque"},
    {"ticker": "^TNX", "name": "Taux US 10 ans", "group": "Taux"},
    {"ticker": "CL=F", "name": "Petrole WTI", "group": "Matieres premieres"},
    {"ticker": "GC=F", "name": "Or", "group": "Matieres premieres"},
    {"ticker": "BTC-USD", "name": "Bitcoin", "group": "Crypto"},
    {"ticker": "EURUSD=X", "name": "EUR/USD", "group": "Devises"},
]
PASSWORD_MIN_LENGTH = 12
PASSWORD_HASH_ITERATIONS = 600_000
MAX_LOGIN_ATTEMPTS = 5
LOGIN_LOCKOUT_SECONDS = 15 * 60
ACCESS_TOKEN_TTL_SECONDS = 5 * 60
PERSISTENT_SESSION_DAYS = 30
SESSION_TIMEOUT_SECONDS = PERSISTENT_SESSION_DAYS * 24 * 60 * 60
ACCESS_QUERY_PARAM = "access"
REFRESH_QUERY_PARAM = "refresh"
LEGACY_SESSION_QUERY_PARAM = "session"
REFRESH_COOKIE_NAME = "rafik_refresh"
REFRESH_COOKIE_MAX_AGE_SECONDS = PERSISTENT_SESSION_DAYS * 24 * 60 * 60
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"

MAJOR_INDICES = [
    {"ticker": "^GSPC", "name": "S&P 500", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^IXIC", "name": "Nasdaq Composite", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "URTH", "name": "MSCI World", "exchange": "ETF proxy", "asset_type": "Indice"},
    {"ticker": "^DJI", "name": "Dow Jones Industrial Average", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^RUT", "name": "Russell 2000", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^VIX", "name": "CBOE Volatility Index", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^FCHI", "name": "CAC 40", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^STOXX50E", "name": "Euro Stoxx 50", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^GDAXI", "name": "DAX", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^FTSE", "name": "FTSE 100", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^N225", "name": "Nikkei 225", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^HSI", "name": "Hang Seng", "exchange": "Index", "asset_type": "Indice"},
]

EUROPE_LEADERS = [
    # France - CAC 40 (Sélection majeure)
    {"ticker": "MC.PA", "name": "LVMH", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "OR.PA", "name": "L'Oréal", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "TTE.PA", "name": "TotalEnergies", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "SAN.PA", "name": "Sanofi", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "AIR.PA", "name": "Airbus", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "RMS.PA", "name": "Hermès", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "KER.PA", "name": "Kering", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "BNP.PA", "name": "BNP Paribas", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "EL.PA", "name": "EssilorLuxottica", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "DG.PA", "name": "VINCI", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    {"ticker": "SU.PA", "name": "Schneider Electric", "exchange": "Euronext Paris", "asset_type": "Entreprise"},
    # Allemagne - DAX (Sélection majeure)
    {"ticker": "SAP.DE", "name": "SAP", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "SIE.DE", "name": "Siemens", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "ALV.DE", "name": "Allianz", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "DTE.DE", "name": "Deutsche Telekom", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "MBG.DE", "name": "Mercedes-Benz", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "VOW3.DE", "name": "Volkswagen", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "BMW.DE", "name": "BMW", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "BAS.DE", "name": "BASF", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "BAYN.DE", "name": "Bayer", "exchange": "XETRA", "asset_type": "Entreprise"},
    {"ticker": "ADS.DE", "name": "Adidas", "exchange": "XETRA", "asset_type": "Entreprise"},
    # Autres leaders Européens
    {"ticker": "ASML.AS", "name": "ASML Holding", "exchange": "Euronext Amsterdam", "asset_type": "Entreprise"},
]

PERIOD_OPTIONS = {
    "1 jour": {"period": "2d", "interval": "5m"},
    "5 jours": {"period": "6d", "interval": "30m"},
    "1 mois": {"period": "1mo", "interval": "1d"},
    "3 mois": {"period": "3mo", "interval": "1d"},
    "6 mois": {"period": "6mo", "interval": "1d"},
    "1 an": {"period": "1y", "interval": "1d"},
    "2 ans": {"period": "2y", "interval": "1wk"},
    "5 ans": {"period": "5y", "interval": "1wk"},
    "Maximum": {"period": "max", "interval": "1mo"},
}

DEFAULT_TICKERS = ["^GSPC", "^GDAXI", "NVDA", "META", "MSFT", "^FCHI"]
OTHER_EXCHANGE_NAMES = {
    "A": "NYSE American",
    "N": "NYSE",
    "P": "NYSE Arca",
    "Z": "Cboe BZX",
    "V": "IEX",
}
GENERAL_NEWS_FEEDS = {
    "A la une": [
        {"label": "Franceinfo - Les titres", "url": "https://www.francetvinfo.fr/titres.rss"},
        {"label": "Le Monde - A la une", "url": "https://www.lemonde.fr/rss/une.xml"},
        {"label": "Le Figaro - Actualites", "url": "https://www.lefigaro.fr/rss/figaro_actualites.xml"},
        {"label": "20 Minutes - A la une", "url": "https://www.20minutes.fr/feeds/rss-une.xml"},
        {"label": "BFMTV - Actualites", "url": "https://www.bfmtv.com/rss/news-24-7/"},
        {"label": "France 24 - Fil general", "url": "https://www.france24.com/fr/rss"},
        {"label": "RFI - Fil general", "url": "https://www.rfi.fr/fr/rss"},
    ],
    "Politique": [
        {"label": "Le Monde - Politique", "url": "https://www.lemonde.fr/politique/rss_full.xml"},
        {"label": "Le Figaro - Politique", "url": "https://www.lefigaro.fr/rss/figaro_politique.xml"},
        {"label": "BFMTV - Politique", "url": "https://www.bfmtv.com/rss/politique/"},
        {"label": "France 24 - France", "url": "https://www.france24.com/fr/france/rss"},
    ],
    "International": [
        {"label": "France 24 - Fil general", "url": "https://www.france24.com/fr/rss"},
        {"label": "France 24 - Europe", "url": "https://www.france24.com/fr/europe/rss"},
        {"label": "France 24 - Afrique", "url": "https://www.france24.com/fr/afrique/rss"},
        {"label": "Le Monde - International", "url": "https://www.lemonde.fr/international/rss_full.xml"},
        {"label": "Le Figaro - International", "url": "https://www.lefigaro.fr/rss/figaro_international.xml"},
        {"label": "BFMTV - International", "url": "https://www.bfmtv.com/rss/international/"},
        {"label": "RFI - Afrique", "url": "https://www.rfi.fr/fr/afrique/rss"},
        {"label": "RFI - Europe", "url": "https://www.rfi.fr/fr/europe/rss"},
    ],
    "Economie": [
        {"label": "Le Monde - Economie", "url": "https://www.lemonde.fr/economie/rss_full.xml"},
        {"label": "Le Figaro - Economie", "url": "https://www.lefigaro.fr/rss/figaro_economie.xml"},
        {"label": "BFM Business - Economie", "url": "https://www.bfmtv.com/rss/economie/"},
        {"label": "BFM Business - Entreprises", "url": "https://www.bfmtv.com/rss/economie/entreprises/"},
        {"label": "BFM Bourse", "url": "https://www.bfmtv.com/rss/bourse/"},
        {"label": "France 24 - Economie", "url": "https://www.france24.com/fr/%C3%A9co-tech/rss"},
        {"label": "Les Echos - Une", "url": "https://www.lesechos.fr/rss/rss_une.xml"},
        {"label": "Les Echos - Finance", "url": "https://www.lesechos.fr/rss/rss_finance-marches.xml"},
        {"label": "Challenges - Economie", "url": "https://www.challenges.fr/rss.xml"},
    ],
    "Tech / Sciences": [
        {"label": "Le Monde - Pixels", "url": "https://www.lemonde.fr/pixels/rss_full.xml"},
        {"label": "Le Monde - Sciences", "url": "https://www.lemonde.fr/sciences/rss_full.xml"},
        {"label": "BFMTV - Tech", "url": "https://www.bfmtv.com/rss/tech/"},
        {"label": "BFMTV - Sciences", "url": "https://www.bfmtv.com/rss/sciences/"},
        {"label": "France 24 - Tech", "url": "https://www.france24.com/fr/%C3%A9co-tech/rss"},
    ],
    "Culture / Societe": [
        {"label": "Franceinfo - Les titres", "url": "https://www.francetvinfo.fr/titres.rss"},
        {"label": "Le Monde - Culture", "url": "https://www.lemonde.fr/culture/rss_full.xml"},
        {"label": "Le Figaro - Culture", "url": "https://www.lefigaro.fr/rss/figaro_culture.xml"},
        {"label": "BFMTV - Societe", "url": "https://www.bfmtv.com/rss/societe/"},
        {"label": "BFMTV - Culture", "url": "https://www.bfmtv.com/rss/culture/"},
        {"label": "20 Minutes - Culture", "url": "https://www.20minutes.fr/feeds/rss-culture.xml"},
    ],
    "France": [
        {"label": "Franceinfo - Les titres", "url": "https://www.francetvinfo.fr/titres.rss"},
        {"label": "France 24 - France", "url": "https://www.france24.com/fr/france/rss"},
        {"label": "Le Monde - France", "url": "https://www.lemonde.fr/societe/rss_full.xml"},
        {"label": "Le Figaro - France", "url": "https://www.lefigaro.fr/rss/figaro_actualites.xml"},
        {"label": "BFMTV - Societe", "url": "https://www.bfmtv.com/rss/societe/"},
        {"label": "BFMTV - Police-Justice", "url": "https://www.bfmtv.com/rss/police-justice/"},
    ],
}
NEWS_RECAP_DEFAULT_CATEGORIES = tuple(GENERAL_NEWS_FEEDS.keys())
NEWS_RECAP_DEFAULT_LIMIT = 6
PODCAST_DEFAULT_DURATION_MINUTES = 10
PODCAST_DURATION_OPTIONS = [3, 5, 10]
PODCAST_TTS_MODEL = "gpt-4o-mini-tts"
PODCAST_SCRIPT_MODEL = os.getenv("OPENAI_SCRIPT_MODEL", "gpt-4o-mini")
NEWS_DIGEST_CACHE_KEY = "daily_news_digest"
EDITORIAL_STATE_CACHE_KEY = "editorial_state"
NEWS_DIGEST_DEBUG_CACHE_KEY = "news_digest_debug"
NEWS_DIGEST_REUSE_MINUTES = 60
NEWS_DIGEST_MAIN_SWITCH_MARGIN = 18.0


load_local_env_file(BASE_DIR / ".env")


st.set_page_config(page_title="Rafik Moulouel", layout="wide")


inject_global_styles()


EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
VALID_ROLES = ("user", "admin")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(timezone.utc).isoformat()


def parse_utc_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_username(value: str) -> str:
    return (value or "").strip().lower()


def validate_username(username: str) -> list[str]:
    normalized = normalize_username(username)
    if len(normalized) > 254 or not EMAIL_PATTERN.fullmatch(normalized):
        return ["Saisis une adresse email valide."]
    return []


def validate_display_name(display_name: str) -> list[str]:
    cleaned = (display_name or "").strip()
    if len(cleaned) < 2:
        return ["Le nom affiche doit contenir au moins 2 caracteres."]
    if len(cleaned) > 80:
        return ["Le nom affiche ne doit pas depasser 80 caracteres."]
    return []


def validate_password(password: str, username: str = "", display_name: str = "") -> list[str]:
    password = password or ""
    errors: list[str] = []
    if len(password) < PASSWORD_MIN_LENGTH:
        errors.append(f"Le mot de passe doit contenir au moins {PASSWORD_MIN_LENGTH} caracteres.")

    category_count = sum(
        [
            any(char.islower() for char in password),
            any(char.isupper() for char in password),
            any(char.isdigit() for char in password),
            any(not char.isalnum() for char in password),
        ]
    )
    if category_count < 3:
        errors.append("Utilise au moins 3 familles de caracteres : minuscules, majuscules, chiffres, symboles.")

    lower_password = password.lower()
    normalized_username = normalize_username(username)
    if normalized_username and normalized_username in lower_password:
        errors.append("Le mot de passe ne doit pas contenir l'adresse email.")

    for part in re.split(r"\s+", (display_name or "").strip().lower()):
        if len(part) >= 4 and part in lower_password:
            errors.append("Le mot de passe ne doit pas contenir le nom affiche.")
            break

    weak_terms = ("password", "motdepasse", "azerty", "qwerty", "123456", "admin")
    if any(term in lower_password for term in weak_terms):
        errors.append("Evite les mots de passe trop courants ou previsibles.")

    if len(set(password)) < 5:
        errors.append("Le mot de passe doit etre plus varie.")

    return errors


def hash_password(password: str) -> str:
    salt = pysecrets.token_bytes(16)
    password_hash = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return f"pbkdf2_sha256${PASSWORD_HASH_ITERATIONS}${salt.hex()}${password_hash.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations_raw, salt_hex, expected_hash_hex = stored_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
        salt = bytes.fromhex(salt_hex)
        expected_hash = bytes.fromhex(expected_hash_hex)
    except (AttributeError, TypeError, ValueError):
        return False

    candidate_hash = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(candidate_hash, expected_hash)


def get_user_db_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(exist_ok=True)
    connection = sqlite3.connect(USER_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    try:
        USER_DB_PATH.chmod(0o600)
    except OSError:
        pass
    return connection


def init_user_db() -> None:
    with get_user_db_connection() as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE COLLATE NOCASE,
                display_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('user', 'admin')),
                is_active INTEGER NOT NULL DEFAULT 1,
                must_change_password INTEGER NOT NULL DEFAULT 0,
                failed_login_count INTEGER NOT NULL DEFAULT 0,
                locked_until TEXT,
                last_login_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                created_by TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred_at TEXT NOT NULL,
                actor_username TEXT,
                event_type TEXT NOT NULL,
                target_username TEXT,
                details TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT,
                FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                ticker TEXT NOT NULL,
                quantity REAL NOT NULL CHECK(quantity > 0),
                purchase_price REAL NOT NULL CHECK(purchase_price > 0),
                purchase_date TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(username) REFERENCES users(username) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_audit_log_occurred_at ON auth_audit_log(occurred_at)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_username ON auth_sessions(username)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_token_hash ON auth_sessions(token_hash)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_portfolio_positions_username ON portfolio_positions(username)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS email_schedule (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                enabled INTEGER NOT NULL DEFAULT 0,
                recipients TEXT NOT NULL DEFAULT '["rafik.mo1995@gmail.com"]',
                send_time TEXT NOT NULL DEFAULT '08:00',
                last_sent_date TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        for migration in [
            "ALTER TABLE email_schedule ADD COLUMN recipients TEXT NOT NULL DEFAULT '[\"rafik.mo1995@gmail.com\"]'",
            "ALTER TABLE email_schedule ADD COLUMN send_time TEXT NOT NULL DEFAULT '08:00'",
            "ALTER TABLE email_schedule ADD COLUMN last_sent_date TEXT NOT NULL DEFAULT ''",
        ]:
            try:
                connection.execute(migration)
            except Exception:
                pass

def execute_query(query: str, params: tuple = (), commit: bool = False):
    """Utilitaire pour centraliser l'exécution SQL et la gestion d'erreurs."""
    try:
        with get_user_db_connection() as conn:
            cursor = conn.execute(query, params)
            if commit:
                conn.commit()
            return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Database error: {e} | Query: {query}")
        raise

def get_email_schedule() -> dict:
    import json as _json
    with get_user_db_connection() as conn:
        row = conn.execute(
            "SELECT enabled, recipients, send_time, last_sent_date FROM email_schedule WHERE id = 1"
        ).fetchone()
    if row:
        try:
            recipients = _json.loads(row["recipients"])
        except Exception:
            recipients = [row["recipients"]] if row["recipients"] else ["rafik.mo1995@gmail.com"]
        return {
            "enabled": bool(row["enabled"]),
            "recipients": recipients,
            "send_time": row["send_time"] or "08:00",
            "last_sent_date": row["last_sent_date"] or "",
        }
    return {"enabled": False, "recipients": ["rafik.mo1995@gmail.com"], "send_time": "08:00", "last_sent_date": ""}


def save_email_schedule(enabled: bool, recipients: list[str], send_time: str) -> None:
    import json as _json
    now = datetime.utcnow().isoformat()
    with get_user_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO email_schedule (id, enabled, recipients, send_time, last_sent_date, updated_at)
            VALUES (1, ?, ?, ?, '', ?)
            ON CONFLICT(id) DO UPDATE SET enabled = excluded.enabled,
                                          recipients = excluded.recipients,
                                          send_time = excluded.send_time,
                                          updated_at = excluded.updated_at
            """,
            (int(enabled), _json.dumps(recipients), send_time, now),
        )


def mark_briefing_email_sent_today() -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    with get_user_db_connection() as conn:
        conn.execute("UPDATE email_schedule SET last_sent_date = ? WHERE id = 1", (today,))


def record_audit_event(
    event_type: str,
    actor_username: str | None = None,
    target_username: str | None = None,
    details: str = "",
) -> None:
    try:
        with get_user_db_connection() as connection:
            connection.execute(
                """
                INSERT INTO auth_audit_log (occurred_at, actor_username, event_type, target_username, details)
                VALUES (?, ?, ?, ?, ?)
                """,
                (utc_iso(), actor_username, event_type, target_username, details),
            )
    except sqlite3.Error as e:
        logger.error(f"Audit log failed: {e}")


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def base64url_encode(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def get_jwt_secret() -> bytes:
    DATA_DIR.mkdir(exist_ok=True)
    if JWT_SECRET_PATH.exists():
        secret = JWT_SECRET_PATH.read_text(encoding="utf-8").strip()
        if secret:
            return secret.encode("utf-8")

    secret = pysecrets.token_urlsafe(48)
    JWT_SECRET_PATH.write_text(secret, encoding="utf-8")
    try:
        JWT_SECRET_PATH.chmod(0o600)
    except OSError:
        pass
    return secret.encode("utf-8")


def create_access_jwt(user: sqlite3.Row) -> str:
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": user["username"],
        "typ": "access",
        "iat": now,
        "exp": now + ACCESS_TOKEN_TTL_SECONDS,
        "jti": pysecrets.token_urlsafe(12),
    }
    encoded_header = base64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    encoded_payload = base64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{encoded_header}.{encoded_payload}"
    signature = hmac.new(get_jwt_secret(), signing_input.encode("ascii"), hashlib.sha256).digest()
    return f"{signing_input}.{base64url_encode(signature)}"


def verify_access_jwt(token: str) -> dict | None:
    if not token:
        return None
    try:
        encoded_header, encoded_payload, encoded_signature = token.split(".", 2)
        signing_input = f"{encoded_header}.{encoded_payload}"
        expected_signature = hmac.new(get_jwt_secret(), signing_input.encode("ascii"), hashlib.sha256).digest()
        signature = base64url_decode(encoded_signature)
        if not hmac.compare_digest(signature, expected_signature):
            return None

        header = json.loads(base64url_decode(encoded_header))
        payload = json.loads(base64url_decode(encoded_payload))
    except Exception:
        return None

    if header.get("alg") != "HS256" or payload.get("typ") != "access":
        return None
    exp = payload.get("exp")
    sub = payload.get("sub")
    if not isinstance(exp, int) or exp <= int(time.time()) or not isinstance(sub, str):
        return None
    return payload


def get_query_param_value(name: str) -> str:
    try:
        value = st.query_params.get(name, "")
    except Exception:
        return ""
    if isinstance(value, list):
        return str(value[0] if value else "").strip()
    return str(value or "").strip()


def get_access_token_from_query() -> str:
    return get_query_param_value(ACCESS_QUERY_PARAM)


def get_refresh_token_from_query() -> str:
    return get_query_param_value(REFRESH_QUERY_PARAM) or get_query_param_value(LEGACY_SESSION_QUERY_PARAM)


def get_refresh_token_from_cookie() -> str:
    try:
        return str(st.context.cookies.get(REFRESH_COOKIE_NAME, "") or "").strip()
    except Exception:
        return ""


def get_refresh_token_from_request() -> str:
    return get_refresh_token_from_cookie() or get_refresh_token_from_query()


def queue_refresh_cookie_update(refresh_token: str) -> None:
    st.session_state["auth_refresh_cookie_value"] = refresh_token
    st.session_state["auth_refresh_cookie_clear"] = False


def queue_refresh_cookie_clear() -> None:
    st.session_state["auth_refresh_cookie_value"] = ""
    st.session_state["auth_refresh_cookie_clear"] = True


def render_auth_cookie_sync() -> None:
    should_clear = bool(st.session_state.pop("auth_refresh_cookie_clear", False))
    refresh_token = str(st.session_state.pop("auth_refresh_cookie_value", "") or "")
    should_clean_url = bool(get_access_token_from_query() or get_refresh_token_from_query())
    if not should_clear and not refresh_token and not should_clean_url:
        return

    cookie_name = json.dumps(REFRESH_COOKIE_NAME)
    access_param = json.dumps(ACCESS_QUERY_PARAM)
    refresh_param = json.dumps(REFRESH_QUERY_PARAM)
    legacy_param = json.dumps(LEGACY_SESSION_QUERY_PARAM)
    if should_clear:
        cookie_action = (
            f"document.cookie = {cookie_name} + '=; Max-Age=0; Path=/; SameSite=Lax';"
        )
    else:
        cookie_action = (
            f"document.cookie = {cookie_name} + '=' + encodeURIComponent({json.dumps(refresh_token)}) + "
            f"'; Max-Age={REFRESH_COOKIE_MAX_AGE_SECONDS}; Path=/; SameSite=Lax';"
        )

    st.html(
        f"""
        <script>
        (function() {{
            {cookie_action}
            const url = new URL(window.location.href);
            [{access_param}, {refresh_param}, {legacy_param}].forEach((name) => url.searchParams.delete(name));
            const cleanUrl = url.pathname + (url.search ? url.search : '') + url.hash;
            if (cleanUrl !== window.location.pathname + window.location.search + window.location.hash) {{
                window.history.replaceState({{}}, document.title, cleanUrl);
            }}
        }})();
        </script>
        """
    )


def clear_auth_tokens_from_query() -> None:
    try:
        for name in (ACCESS_QUERY_PARAM, REFRESH_QUERY_PARAM, LEGACY_SESSION_QUERY_PARAM):
            if name in st.query_params:
                del st.query_params[name]
    except Exception:
        pass


def create_persistent_session(username: str) -> str:
    normalized = normalize_username(username)
    token = pysecrets.token_urlsafe(32)
    token_hash = hash_session_token(token)
    now = utc_now()
    expires_at = now + timedelta(days=PERSISTENT_SESSION_DAYS)
    with get_user_db_connection() as connection:
        connection.execute(
            """
            DELETE FROM auth_sessions
            WHERE expires_at <= ? OR revoked_at IS NOT NULL
            """,
            (utc_iso(now),),
        )
        connection.execute(
            """
            INSERT INTO auth_sessions (username, token_hash, created_at, last_used_at, expires_at, revoked_at)
            VALUES (?, ?, ?, ?, ?, NULL)
            """,
            (normalized, token_hash, utc_iso(now), utc_iso(now), utc_iso(expires_at)),
        )
    return token


def revoke_persistent_sessions(username: str, actor_username: str | None = None) -> None:
    normalized = normalize_username(username)
    if not normalized:
        return
    with get_user_db_connection() as connection:
        connection.execute(
            """
            UPDATE auth_sessions
            SET revoked_at = ?
            WHERE username = ? COLLATE NOCASE AND revoked_at IS NULL
            """,
            (utc_iso(), normalized),
        )
    record_audit_event("sessions_revoked", actor_username or normalized, normalized)


def revoke_persistent_session_token(token: str) -> None:
    if not token:
        return
    with get_user_db_connection() as connection:
        connection.execute(
            """
            UPDATE auth_sessions
            SET revoked_at = ?
            WHERE token_hash = ? AND revoked_at IS NULL
            """,
            (utc_iso(), hash_session_token(token)),
        )


def get_user_from_persistent_session(token: str) -> sqlite3.Row | None:
    if not token:
        return None
    token_hash = hash_session_token(token)
    with get_user_db_connection() as connection:
        session_row = connection.execute(
            """
            SELECT username, expires_at, revoked_at
            FROM auth_sessions
            WHERE token_hash = ?
            """,
            (token_hash,),
        ).fetchone()
        if session_row is None:
            return None

        expires_at = parse_utc_datetime(session_row["expires_at"])
        if session_row["revoked_at"] or not expires_at or expires_at <= utc_now():
            connection.execute(
                """
                UPDATE auth_sessions
                SET revoked_at = COALESCE(revoked_at, ?)
                WHERE token_hash = ?
                """,
                (utc_iso(), token_hash),
            )
            return None

        user = connection.execute(
            "SELECT * FROM users WHERE username = ? COLLATE NOCASE",
            (session_row["username"],),
        ).fetchone()
        if user is None or not bool(user["is_active"]):
            connection.execute(
                """
                UPDATE auth_sessions
                SET revoked_at = COALESCE(revoked_at, ?)
                WHERE token_hash = ?
                """,
                (utc_iso(), token_hash),
            )
            return None

        connection.execute(
            "UPDATE auth_sessions SET last_used_at = ? WHERE token_hash = ?",
            (utc_iso(), token_hash),
        )
        return user


def has_any_user() -> bool:
    with get_user_db_connection() as connection:
        row = connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
    return bool(row and row["total"])


def get_user_by_username(username: str) -> sqlite3.Row | None:
    normalized = normalize_username(username)
    if not normalized:
        return None
    with get_user_db_connection() as connection:
        return connection.execute(
            "SELECT * FROM users WHERE username = ? COLLATE NOCASE",
            (normalized,),
        ).fetchone()


def count_active_admins() -> int:
    with get_user_db_connection() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS total FROM users WHERE role = 'admin' AND is_active = 1"
        ).fetchone()
    return int(row["total"] if row else 0)


def list_users() -> list[dict]:
    with get_user_db_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, username, display_name, role, is_active, must_change_password,
                   failed_login_count, locked_until, last_login_at, created_at, updated_at, created_by
            FROM users
            ORDER BY username
            """
        ).fetchall()

    users = [dict(row) for row in rows]
    for user in users:
        user["is_active"] = bool(user["is_active"])
        user["must_change_password"] = bool(user["must_change_password"])
    return users


def list_active_user_recipients() -> list[str]:
    return [user["username"] for user in list_users() if user["is_active"] and EMAIL_PATTERN.fullmatch(user["username"])]


def list_recent_audit_events(limit: int = 40) -> list[dict]:
    with get_user_db_connection() as connection:
        rows = connection.execute(
            """
            SELECT occurred_at, actor_username, event_type, target_username, details
            FROM auth_audit_log
            ORDER BY occurred_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def create_user(
    username: str,
    display_name: str,
    password: str,
    role: str,
    actor_username: str,
    must_change_password: bool = False,
) -> None:
    normalized = normalize_username(username)
    if role not in VALID_ROLES:
        raise ValueError("Role invalide.")
    validation_errors = []
    validation_errors.extend(validate_username(normalized))
    validation_errors.extend(validate_display_name(display_name))
    validation_errors.extend(validate_password(password, normalized, display_name))
    if validation_errors:
        raise ValueError(" ".join(validation_errors))

    now = utc_iso()
    try:
        with get_user_db_connection() as connection:
            connection.execute(
                """
                INSERT INTO users (
                    username, display_name, password_hash, role, is_active, must_change_password,
                    failed_login_count, locked_until, last_login_at, created_at, updated_at, created_by
                )
                VALUES (?, ?, ?, ?, 1, ?, 0, NULL, NULL, ?, ?, ?)
                """,
                (
                    normalized,
                    display_name.strip(),
                    hash_password(password),
                    role,
                    int(must_change_password),
                    now,
                    now,
                    actor_username,
                ),
            )
    except sqlite3.IntegrityError as exc:
        raise ValueError("Cette adresse email existe deja.") from exc

    record_audit_event("user_created", actor_username, normalized, f"role={role}")


def update_user_profile(
    target_username: str,
    display_name: str,
    role: str,
    is_active: bool,
    must_change_password: bool,
    actor_username: str,
) -> None:
    target = get_user_by_username(target_username)
    if target is None:
        raise ValueError("Utilisateur introuvable.")
    if role not in VALID_ROLES:
        raise ValueError("Role invalide.")

    normalized_target = normalize_username(target_username)
    normalized_actor = normalize_username(actor_username)
    target_is_active_admin = target["role"] == "admin" and bool(target["is_active"])
    would_remove_active_admin = target_is_active_admin and (role != "admin" or not is_active)
    if would_remove_active_admin and count_active_admins() <= 1:
        raise ValueError("Impossible de retirer le dernier administrateur actif.")

    if normalized_target == normalized_actor and (role != target["role"] or bool(is_active) != bool(target["is_active"])):
        raise ValueError("Tu ne peux pas modifier ton propre role ou statut.")

    with get_user_db_connection() as connection:
        connection.execute(
            """
            UPDATE users
            SET display_name = ?, role = ?, is_active = ?, must_change_password = ?, updated_at = ?
            WHERE username = ? COLLATE NOCASE
            """,
            (
                display_name.strip(),
                role,
                int(is_active),
                int(must_change_password),
                utc_iso(),
                normalized_target,
            ),
        )

    record_audit_event(
        "user_updated",
        normalized_actor,
        normalized_target,
        f"role={role}, active={int(is_active)}, must_change_password={int(must_change_password)}",
    )
    if not is_active:
        revoke_persistent_sessions(normalized_target, normalized_actor)


def set_user_active_status(target_username: str, is_active: bool, actor_username: str) -> None:
    target = get_user_by_username(target_username)
    if target is None:
        raise ValueError("Utilisateur introuvable.")

    normalized_target = normalize_username(target_username)
    normalized_actor = normalize_username(actor_username)
    if normalized_target == normalized_actor and not is_active:
        raise ValueError("Tu ne peux pas bloquer ton propre compte.")
    if target["role"] == "admin" and bool(target["is_active"]) and not is_active and count_active_admins() <= 1:
        raise ValueError("Impossible de bloquer le dernier administrateur actif.")

    with get_user_db_connection() as connection:
        connection.execute(
            """
            UPDATE users
            SET is_active = ?, failed_login_count = CASE WHEN ? = 1 THEN 0 ELSE failed_login_count END,
                locked_until = CASE WHEN ? = 1 THEN NULL ELSE locked_until END,
                updated_at = ?
            WHERE username = ? COLLATE NOCASE
            """,
            (int(is_active), int(is_active), int(is_active), utc_iso(), normalized_target),
        )

    if not is_active:
        revoke_persistent_sessions(normalized_target, normalized_actor)

    record_audit_event(
        "user_activated" if is_active else "user_blocked",
        normalized_actor,
        normalized_target,
    )


def unlock_user_account(target_username: str, actor_username: str) -> None:
    target = get_user_by_username(target_username)
    if target is None:
        raise ValueError("Utilisateur introuvable.")

    normalized_target = normalize_username(target_username)
    normalized_actor = normalize_username(actor_username)
    with get_user_db_connection() as connection:
        connection.execute(
            """
            UPDATE users
            SET failed_login_count = 0, locked_until = NULL, updated_at = ?
            WHERE username = ? COLLATE NOCASE
            """,
            (utc_iso(), normalized_target),
        )

    record_audit_event("user_unlocked", normalized_actor, normalized_target)


def update_user_password(
    target_username: str,
    new_password: str,
    actor_username: str,
    must_change_password: bool = False,
) -> None:
    normalized_target = normalize_username(target_username)
    target = get_user_by_username(normalized_target)
    if target is None:
        raise ValueError("Utilisateur introuvable.")
    validation_errors = validate_password(new_password, target["username"], target["display_name"])
    if validation_errors:
        raise ValueError(" ".join(validation_errors))

    with get_user_db_connection() as connection:
        connection.execute(
            """
            UPDATE users
            SET password_hash = ?, must_change_password = ?, failed_login_count = 0,
                locked_until = NULL, updated_at = ?
            WHERE username = ? COLLATE NOCASE
            """,
            (
                hash_password(new_password),
                int(must_change_password),
                utc_iso(),
                normalized_target,
            ),
        )

    revoke_persistent_sessions(normalized_target, normalize_username(actor_username))
    event_type = "password_changed" if normalize_username(actor_username) == normalized_target else "password_reset"
    record_audit_event(event_type, normalize_username(actor_username), normalized_target)


def authenticate_user(username: str, password: str) -> tuple[bool, str, sqlite3.Row | None]:
    normalized = normalize_username(username)
    generic_error = "Email ou mot de passe invalide."
    user = get_user_by_username(normalized)
    if user is None:
        time.sleep(0.45)
        record_audit_event("login_failed", normalized, normalized, "unknown_user")
        return False, generic_error, None

    if not bool(user["is_active"]):
        time.sleep(0.45)
        record_audit_event("login_blocked", normalized, normalized, "inactive_account")
        return False, "Ce compte est desactive. Contacte un administrateur.", None

    locked_until = parse_utc_datetime(user["locked_until"])
    if locked_until and locked_until > utc_now():
        remaining_minutes = max(1, int((locked_until - utc_now()).total_seconds() // 60) + 1)
        record_audit_event("login_blocked", normalized, normalized, "temporary_lockout")
        return False, f"Compte temporairement bloque. Reessaie dans environ {remaining_minutes} min.", None

    if verify_password(password, user["password_hash"]):
        with get_user_db_connection() as connection:
            connection.execute(
                """
                UPDATE users
                SET failed_login_count = 0, locked_until = NULL, last_login_at = ?, updated_at = ?
                WHERE username = ? COLLATE NOCASE
                """,
                (utc_iso(), utc_iso(), normalized),
            )
        record_audit_event("login_success", normalized, normalized)
        return True, "Connexion reussie.", get_user_by_username(normalized)

    failed_count = int(user["failed_login_count"] or 0) + 1
    locked_until_value = None
    message = generic_error
    if failed_count >= MAX_LOGIN_ATTEMPTS:
        locked_until_value = utc_iso(utc_now() + timedelta(seconds=LOGIN_LOCKOUT_SECONDS))
        message = f"Trop de tentatives. Le compte est bloque pendant {LOGIN_LOCKOUT_SECONDS // 60} min."

    with get_user_db_connection() as connection:
        connection.execute(
            """
            UPDATE users
            SET failed_login_count = ?, locked_until = ?, updated_at = ?
            WHERE username = ? COLLATE NOCASE
            """,
            (failed_count, locked_until_value, utc_iso(), normalized),
        )
    time.sleep(0.45)
    record_audit_event("login_failed", normalized, normalized, f"failed_count={failed_count}")
    return False, message, None


def user_session_payload(user: sqlite3.Row) -> dict:
    now = utc_iso()
    return {
        "id": int(user["id"]),
        "username": user["username"],
        "display_name": user["display_name"],
        "role": user["role"],
        "must_change_password": bool(user["must_change_password"]),
        "authenticated_at": now,
        "last_seen": now,
    }


def start_authenticated_session(user: sqlite3.Row, persist: bool = True) -> None:
    st.session_state["auth_user"] = user_session_payload(user)
    if persist:
        refresh_token = create_persistent_session(user["username"])
        queue_refresh_cookie_update(refresh_token)
        clear_auth_tokens_from_query()


def clear_authenticated_session(revoke: bool = False) -> None:
    if revoke:
        revoke_persistent_session_token(get_refresh_token_from_request())
    st.session_state.pop("auth_user", None)
    queue_refresh_cookie_clear()
    clear_auth_tokens_from_query()


def get_authenticated_user() -> sqlite3.Row | None:
    session = st.session_state.get("auth_user")
    if not session:
        access_payload = verify_access_jwt(get_access_token_from_query())
        if access_payload is not None:
            user_from_access = get_user_by_username(str(access_payload.get("sub") or ""))
            if user_from_access is not None and bool(user_from_access["is_active"]):
                st.session_state["auth_user"] = user_session_payload(user_from_access)
                refresh_token_from_query = get_refresh_token_from_query()
                if refresh_token_from_query:
                    queue_refresh_cookie_update(refresh_token_from_query)
                clear_auth_tokens_from_query()
                return user_from_access

        refresh_token = get_refresh_token_from_request()
        user_from_refresh = get_user_from_persistent_session(refresh_token)
        if user_from_refresh is None:
            clear_auth_tokens_from_query()
            return None
        if get_refresh_token_from_query():
            queue_refresh_cookie_update(refresh_token)
        clear_auth_tokens_from_query()
        st.session_state["auth_user"] = user_session_payload(user_from_refresh)
        record_audit_event("session_refreshed", user_from_refresh["username"], user_from_refresh["username"])
        return user_from_refresh

    last_seen = parse_utc_datetime(session.get("last_seen")) or parse_utc_datetime(session.get("authenticated_at"))
    if last_seen and utc_now() - last_seen > timedelta(seconds=SESSION_TIMEOUT_SECONDS):
        record_audit_event("session_expired", session.get("username"), session.get("username"))
        clear_authenticated_session()
        return None

    user = get_user_by_username(str(session.get("username") or ""))
    if user is None or not bool(user["is_active"]):
        clear_authenticated_session()
        return None

    session.update(
        {
            "id": int(user["id"]),
            "username": user["username"],
            "display_name": user["display_name"],
            "role": user["role"],
            "must_change_password": bool(user["must_change_password"]),
            "last_seen": utc_iso(),
        }
    )
    st.session_state["auth_user"] = session
    return user


def render_validation_errors(errors: list[str]) -> None:
    if not errors:
        return
    st.error("Corrige ces points avant de continuer :")
    for error in errors:
        st.write(f"- {error}")


def render_initial_admin_setup() -> None:
    st.title("Configuration initiale")
    st.caption("Cree le premier compte administrateur avec une adresse email et un mot de passe.")

    with st.form("initial_admin_setup_form", clear_on_submit=False):
        username = st.text_input("Email admin", placeholder="rafik@example.com")
        display_name = st.text_input("Nom affiche", placeholder="Rafik")
        password = st.text_input("Mot de passe", type="password")
        password_confirm = st.text_input("Confirmation du mot de passe", type="password")
        submitted = st.form_submit_button("Creer le compte admin")

    if not submitted:
        st.info(
            "Conseil : choisis une phrase de passe longue et unique. Elle restera uniquement sous forme hachee dans la base locale."
        )
        return

    errors = []
    errors.extend(validate_username(username))
    errors.extend(validate_display_name(display_name))
    errors.extend(validate_password(password, username, display_name))
    if password != password_confirm:
        errors.append("La confirmation du mot de passe ne correspond pas.")

    if errors:
        render_validation_errors(errors)
        return

    try:
        create_user(username, display_name, password, role="admin", actor_username="system")
    except ValueError as exc:
        st.error(str(exc))
        return

    user = get_user_by_username(username)
    if user is not None:
        start_authenticated_session(user)
    st.success("Compte administrateur cree.")
    st.rerun()


def render_login_screen() -> None:
    st.title("Connexion")
    st.caption("Connecte-toi pour acceder au tableau de bord finance.")

    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Email")
        password = st.text_input("Mot de passe", type="password")
        submitted = st.form_submit_button("Se connecter")

    if submitted:
        success, message, user = authenticate_user(username, password)
        if success and user is not None:
            start_authenticated_session(user)
            st.rerun()
        st.error(message)

    st.caption(
        f"Session renouvelee automatiquement par cookie local, sans jeton dans l'URL. "
        f"Par securite, un compte est temporairement bloque apres {MAX_LOGIN_ATTEMPTS} echecs consecutifs."
    )


def render_mandatory_password_change(user: sqlite3.Row) -> None:
    st.title("Changement de mot de passe requis")
    st.warning("Ton compte utilise un mot de passe temporaire ou a ete marque pour renouvellement.")

    with st.form("mandatory_password_change_form", clear_on_submit=False):
        current_password = st.text_input("Mot de passe actuel", type="password")
        new_password = st.text_input("Nouveau mot de passe", type="password")
        new_password_confirm = st.text_input("Confirmation du nouveau mot de passe", type="password")
        submitted = st.form_submit_button("Mettre a jour mon mot de passe")

    if not submitted:
        return

    if not verify_password(current_password, user["password_hash"]):
        st.error("Mot de passe actuel incorrect.")
        return

    errors = validate_password(new_password, user["username"], user["display_name"])
    if new_password != new_password_confirm:
        errors.append("La confirmation du nouveau mot de passe ne correspond pas.")
    if verify_password(new_password, user["password_hash"]):
        errors.append("Le nouveau mot de passe doit etre different de l'ancien.")

    if errors:
        render_validation_errors(errors)
        return

    update_user_password(user["username"], new_password, user["username"], must_change_password=False)
    refreshed_user = get_user_by_username(user["username"])
    if refreshed_user is not None:
        start_authenticated_session(refreshed_user)
    st.success("Mot de passe mis a jour.")
    st.rerun()


def require_authenticated_user() -> sqlite3.Row | None:
    init_user_db()
    if not has_any_user():
        render_initial_admin_setup()
        return None

    user = get_authenticated_user()
    if user is None:
        render_login_screen()
        return None

    if bool(user["must_change_password"]):
        render_mandatory_password_change(user)
        return None

    return user


def render_account_sidebar(current_user: sqlite3.Row) -> None:
    st.caption(f"Connecte : **{current_user['display_name']}**")
    st.caption(f"Email : `{current_user['username']}` | role : `{current_user['role']}`")

    if st.button("Se deconnecter", use_container_width=True):
        record_audit_event("logout", current_user["username"], current_user["username"])
        clear_authenticated_session(revoke=True)
        st.rerun()

    with st.expander("Changer mon mot de passe"):
        with st.form("own_password_change_form", clear_on_submit=True):
            current_password = st.text_input("Mot de passe actuel", type="password", key="own_current_password")
            new_password = st.text_input("Nouveau mot de passe", type="password", key="own_new_password")
            new_password_confirm = st.text_input(
                "Confirmation",
                type="password",
                key="own_new_password_confirm",
            )
            submitted = st.form_submit_button("Mettre a jour")

        if submitted:
            fresh_user = get_user_by_username(current_user["username"])
            if fresh_user is None or not verify_password(current_password, fresh_user["password_hash"]):
                st.error("Mot de passe actuel incorrect.")
                return

            errors = validate_password(new_password, fresh_user["username"], fresh_user["display_name"])
            if new_password != new_password_confirm:
                errors.append("La confirmation ne correspond pas.")
            if verify_password(new_password, fresh_user["password_hash"]):
                errors.append("Le nouveau mot de passe doit etre different de l'ancien.")

            if errors:
                render_validation_errors(errors)
                return

            update_user_password(fresh_user["username"], new_password, fresh_user["username"])
            refreshed_user = get_user_by_username(fresh_user["username"])
            if refreshed_user is not None:
                start_authenticated_session(refreshed_user)
            st.success("Mot de passe mis a jour.")


def render_user_management_section(current_user: sqlite3.Row) -> None:
    st.title("Admin utilisateurs")
    st.caption("Page reservee aux administrateurs : creation de comptes, roles, blocage, deverrouillage et journal securite.")

    users = list_users()
    active_count = sum(1 for user in users if user["is_active"])
    admin_count = sum(1 for user in users if user["role"] == "admin" and user["is_active"])
    locked_count = sum(1 for user in users if parse_utc_datetime(user.get("locked_until")) and parse_utc_datetime(user.get("locked_until")) > utc_now())

    metric_col1, metric_col2, metric_col3 = st.columns(3)
    metric_col1.metric("Comptes actifs", active_count)
    metric_col2.metric("Admins actifs", admin_count)
    metric_col3.metric("Comptes bloques", locked_count)

    if users:
        st.subheader("Vue d'ensemble")
        table = pd.DataFrame(
            [
                {
                    "Email": user["username"],
                    "Nom": user["display_name"],
                    "Role": user["role"],
                    "Actif": "Oui" if user["is_active"] else "Non",
                    "Changement MDP": "Oui" if user["must_change_password"] else "Non",
                    "Echecs": user["failed_login_count"],
                    "Bloque jusqu'a": user["locked_until"] or "-",
                    "Derniere connexion": user["last_login_at"] or "-",
                    "Cree par": user["created_by"] or "-",
                }
                for user in users
            ]
        )
        st.dataframe(table, width="stretch", hide_index=True)

    with st.expander("Ajouter un utilisateur", expanded=True):
        with st.form("create_user_form", clear_on_submit=True):
            new_username = st.text_input("Email", placeholder="prenom.nom@example.com")
            new_display_name = st.text_input("Nom affiche", placeholder="Prenom Nom")
            new_role = st.selectbox("Role", options=list(VALID_ROLES), index=0)
            new_password = st.text_input("Mot de passe temporaire", type="password")
            new_password_confirm = st.text_input("Confirmation", type="password")
            new_must_change = st.checkbox("Forcer le changement au premier login", value=True)
            submitted_create = st.form_submit_button("Creer l'utilisateur")

        if submitted_create:
            errors = []
            errors.extend(validate_username(new_username))
            errors.extend(validate_display_name(new_display_name))
            errors.extend(validate_password(new_password, new_username, new_display_name))
            if new_password != new_password_confirm:
                errors.append("La confirmation du mot de passe ne correspond pas.")

            if errors:
                render_validation_errors(errors)
            else:
                try:
                    create_user(
                        new_username,
                        new_display_name,
                        new_password,
                        role=new_role,
                        actor_username=current_user["username"],
                        must_change_password=new_must_change,
                    )
                    st.success("Utilisateur cree.")
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))

    if not users:
        return

    selected_username = st.selectbox(
        "Compte a modifier",
        options=[user["username"] for user in users],
        format_func=lambda username: next(
            f"{user['display_name']} ({user['username']})" for user in users if user["username"] == username
        ),
    )
    selected_user = get_user_by_username(selected_username)
    if selected_user is None:
        st.info("Selection indisponible.")
        return

    st.subheader("Gerer le compte selectionne")
    locked_until = parse_utc_datetime(selected_user["locked_until"])
    is_temporarily_locked = bool(locked_until and locked_until > utc_now())
    status_col1, status_col2, status_col3, status_col4 = st.columns(4)
    status_col1.metric("Email", selected_user["username"])
    status_col2.metric("Role", selected_user["role"])
    status_col3.metric("Statut", "Actif" if bool(selected_user["is_active"]) else "Bloque")
    status_col4.metric("Echecs login", int(selected_user["failed_login_count"] or 0))

    action_col1, action_col2, action_col3 = st.columns(3)
    if action_col1.button("Bloquer le compte", disabled=not bool(selected_user["is_active"]), use_container_width=True):
        try:
            set_user_active_status(selected_user["username"], False, current_user["username"])
            st.success("Compte bloque.")
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

    if action_col2.button("Reactiver le compte", disabled=bool(selected_user["is_active"]), use_container_width=True):
        try:
            set_user_active_status(selected_user["username"], True, current_user["username"])
            st.success("Compte reactive.")
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

    if action_col3.button("Deverrouiller les echecs", disabled=not is_temporarily_locked and int(selected_user["failed_login_count"] or 0) == 0, use_container_width=True):
        try:
            unlock_user_account(selected_user["username"], current_user["username"])
            st.success("Echecs de connexion remis a zero.")
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

    with st.form("update_user_form", clear_on_submit=False):
        display_name = st.text_input("Nom affiche", value=selected_user["display_name"])
        role = st.selectbox(
            "Role",
            options=list(VALID_ROLES),
            index=list(VALID_ROLES).index(selected_user["role"]),
        )
        is_active = st.toggle("Compte actif", value=bool(selected_user["is_active"]))
        must_change_password = st.toggle(
            "Forcer un changement de mot de passe",
            value=bool(selected_user["must_change_password"]),
        )
        submitted_update = st.form_submit_button("Enregistrer les modifications")

    if submitted_update:
        errors = validate_display_name(display_name)
        if errors:
            render_validation_errors(errors)
        else:
            try:
                update_user_profile(
                    selected_username,
                    display_name,
                    role,
                    is_active,
                    must_change_password,
                    current_user["username"],
                )
                st.success("Utilisateur mis a jour.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))

    with st.form("reset_user_password_form", clear_on_submit=True):
        st.write("Reinitialiser le mot de passe")
        temp_password = st.text_input("Nouveau mot de passe temporaire", type="password")
        temp_password_confirm = st.text_input("Confirmation du temporaire", type="password")
        submitted_reset = st.form_submit_button("Reinitialiser")

    if submitted_reset:
        errors = validate_password(temp_password, selected_user["username"], selected_user["display_name"])
        if temp_password != temp_password_confirm:
            errors.append("La confirmation du mot de passe temporaire ne correspond pas.")
        if errors:
            render_validation_errors(errors)
        else:
            try:
                update_user_password(
                    selected_user["username"],
                    temp_password,
                    current_user["username"],
                    must_change_password=True,
                )
                st.success("Mot de passe reinitialise. L'utilisateur devra le changer a sa prochaine connexion.")
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))

    with st.expander("Journal securite recent"):
        audit_events = list_recent_audit_events()
        if not audit_events:
            st.info("Aucun evenement enregistre.")
        else:
            audit_table = pd.DataFrame(
                [
                    {
                        "Date UTC": event["occurred_at"],
                        "Acteur": event["actor_username"] or "-",
                        "Evenement": event["event_type"],
                        "Cible": event["target_username"] or "-",
                        "Details": event["details"] or "-",
                    }
                    for event in audit_events
                ]
            )
            st.dataframe(audit_table, width="stretch", hide_index=True)


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(exist_ok=True)


def market_data_is_ready() -> tuple[bool, str]:
    if yf is None:
        return False, "Le package yfinance n'est pas installe sur la machine."
    return True, ""


def parse_nasdaq_directory(payload: str) -> pd.DataFrame:
    frame = pd.read_csv(StringIO(payload), sep="|")
    frame = frame.rename(columns={"Symbol": "ticker", "Security Name": "name"})
    frame = frame[frame["ticker"].notna()]
    frame = frame[~frame["ticker"].astype(str).str.contains("File Creation Time", na=False)]
    frame = frame[frame["Test Issue"] == "N"]
    frame["ticker"] = frame["ticker"].astype(str).str.strip().str.upper()
    frame["name"] = frame["name"].astype(str).str.strip()
    frame["exchange"] = "Nasdaq"
    frame["asset_type"] = frame["ETF"].map({"Y": "ETF", "N": "Entreprise"}).fillna("Entreprise")
    frame["region"] = "US"
    frame["market_region"] = "US"
    frame["country"] = "United States"
    frame["currency"] = "USD"
    return frame[["ticker", "name", "exchange", "asset_type", "region", "market_region", "country", "currency"]]


def parse_other_listed_directory(payload: str) -> pd.DataFrame:
    frame = pd.read_csv(StringIO(payload), sep="|")
    frame = frame.rename(columns={"ACT Symbol": "ticker", "Security Name": "name"})
    frame = frame[frame["ticker"].notna()]
    frame = frame[~frame["ticker"].astype(str).str.contains("File Creation Time", na=False)]
    frame = frame[frame["Test Issue"] == "N"]
    frame["ticker"] = frame["ticker"].astype(str).str.strip().str.upper()
    frame["name"] = frame["name"].astype(str).str.strip()
    frame["exchange"] = frame["Exchange"].map(OTHER_EXCHANGE_NAMES).fillna(frame["Exchange"])
    frame["asset_type"] = frame["ETF"].map({"Y": "ETF", "N": "Entreprise"}).fillna("Entreprise")
    frame["region"] = "US"
    frame["market_region"] = "US"
    frame["country"] = "United States"
    frame["currency"] = "USD"
    return frame[["ticker", "name", "exchange", "asset_type", "region", "market_region", "country", "currency"]]


def download_crypto_directory(force_refresh: bool = False) -> Path:
    ensure_data_dir()
    cache_exists = CRYPTO_CACHE_PATH.exists()
    cache_is_fresh = cache_exists and (time.time() - CRYPTO_CACHE_PATH.stat().st_mtime) < CRYPTO_CACHE_TTL_SECONDS
    if cache_is_fresh and not force_refresh:
        return CRYPTO_CACHE_PATH

    try:
        response = requests.get(
            COINGECKO_MARKETS_URL,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": "false",
            },
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        cryptos = pd.DataFrame(
            [
                {
                    "ticker": f"{item['symbol'].upper()}-USD",
                    "name": item["name"],
                    "exchange": "Crypto",
                    "asset_type": "Crypto",
                    "region": "Crypto",
                    "market_region": "Crypto",
                    "country": "Crypto",
                    "currency": "USD",
                }
                for item in payload
            ]
        )
        cryptos = cryptos.drop_duplicates(subset=["ticker"], keep="first")
        cryptos.to_csv(CRYPTO_CACHE_PATH, index=False)
        return CRYPTO_CACHE_PATH
    except Exception:
        if cache_exists:
            return CRYPTO_CACHE_PATH
        raise


def download_company_directory(force_refresh: bool = False) -> Path:
    ensure_data_dir()
    cache_exists = MARKET_CACHE_PATH.exists()
    cache_is_fresh = cache_exists and (time.time() - MARKET_CACHE_PATH.stat().st_mtime) < MARKET_CACHE_TTL_SECONDS
    if cache_is_fresh and not force_refresh:
        return MARKET_CACHE_PATH

    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    try:
        nasdaq_response = requests.get(NASDAQ_LISTED_URL, headers=headers, timeout=30)
        nasdaq_response.raise_for_status()
        other_response = requests.get(OTHER_LISTED_URL, headers=headers, timeout=30)
        other_response.raise_for_status()

        companies = pd.concat(
            [
                parse_nasdaq_directory(nasdaq_response.text),
                parse_other_listed_directory(other_response.text),
            ],
            ignore_index=True,
        )
        companies = companies.drop_duplicates(subset=["ticker"], keep="first")
        companies.to_csv(MARKET_CACHE_PATH, index=False)
        return MARKET_CACHE_PATH
    except Exception:
        if cache_exists:
            return MARKET_CACHE_PATH
        raise


@st.cache_data(show_spinner=False)
def load_symbol_catalog(cache_marker: float) -> pd.DataFrame:
    _ = cache_marker
    companies = pd.read_csv(MARKET_CACHE_PATH)
    companies["cik"] = ""
    companies["region"] = companies.get("region", "US")
    companies["market_region"] = companies.get("market_region", "US")
    companies["country"] = companies.get("country", "United States")
    companies["currency"] = companies.get("currency", "USD")

    cryptos = pd.read_csv(CRYPTO_CACHE_PATH)
    cryptos["cik"] = ""
    cryptos["region"] = cryptos.get("region", "Crypto")
    cryptos["market_region"] = cryptos.get("market_region", "Crypto")
    cryptos["country"] = cryptos.get("country", "Crypto")
    cryptos["currency"] = cryptos.get("currency", "USD")

    indices = pd.DataFrame(MAJOR_INDICES)
    indices["cik"] = ""
    indices["region"] = "Global"
    indices["market_region"] = "Global"
    indices["country"] = "Global"
    indices["currency"] = "Mixed"

    euro_stocks = europe_equities_frame()
    euro_stocks["cik"] = ""

    catalog = pd.concat([companies, indices, cryptos, euro_stocks], ignore_index=True, sort=False)
    catalog["ticker"] = catalog["ticker"].astype(str).str.strip().str.upper()
    catalog["name"] = catalog["name"].astype(str).str.strip()
    catalog["exchange"] = catalog["exchange"].fillna("")
    catalog["asset_type"] = catalog["asset_type"].fillna("Entreprise")
    catalog["region"] = catalog["region"].fillna(catalog["asset_type"]).astype(str)
    catalog["market_region"] = catalog.apply(
        lambda row: row.get("market_region") or infer_market_region(row["ticker"], row.get("asset_type")),
        axis=1,
    )
    catalog["country"] = catalog.get("country", "").fillna("")
    catalog["currency"] = catalog.apply(
        lambda row: row.get("currency") or infer_currency(row["ticker"], default="USD"),
        axis=1,
    )
    catalog["label"] = catalog.apply(
        lambda row: f"[{row['region']}] {row['name']} ({row['ticker']}) - {row['exchange'] or row['asset_type']}",
        axis=1,
    )
    catalog = catalog.drop_duplicates(subset=["ticker"], keep="first")
    return catalog.sort_values(["asset_type", "name"], ascending=[True, True]).reset_index(drop=True)


def extract_history_series(
    raw: pd.DataFrame,
    tickers: tuple[str, ...],
    preferred_field: str,
    fallback_field: str = "Close",
) -> pd.DataFrame:
    series_map: dict[str, pd.Series] = {}
    for ticker in tickers:
        if isinstance(raw.columns, pd.MultiIndex):
            if ticker not in raw.columns.get_level_values(0):
                continue
            frame = raw[ticker]
        else:
            frame = raw

        field = preferred_field if preferred_field in frame else fallback_field
        if field not in frame:
            continue

        series = frame[field].dropna()
        # Si la série est vide (cas fréquent de l'Adj Close sur les indices intraday),
        # on tente de basculer sur le fallback immédiat
        if series.empty and field != fallback_field and fallback_field in frame:
            series = frame[fallback_field].dropna()

        if not series.empty:
            series_map[ticker] = series

    history = pd.DataFrame(series_map).sort_index()
    if history.empty:
        raise ValueError("Impossible de construire un historique de prix exploitable.")
    return history


@st.cache_data(ttl=300, show_spinner=False)
def download_price_histories(
    tickers: tuple[str, ...],
    period: str,
    interval: str,
    include_prepost: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if yf is None:
        raise RuntimeError("yfinance n'est pas disponible.")

    raw = yf.download(
        tickers=list(tickers),
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=True,
        group_by="ticker",
        multi_level_index=True,
        prepost=include_prepost,
    )
    if raw.empty:
        raise ValueError("Aucune donnee de marche n'a ete renvoyee.")

    close_history = extract_history_series(raw, tickers, preferred_field="Close")
    return_history = extract_history_series(raw, tickers, preferred_field="Adj Close", fallback_field="Close")
    return close_history, return_history


def compute_performance_frame(history: pd.DataFrame) -> pd.DataFrame:
    performance = pd.DataFrame(index=history.index)
    for column in history.columns:
        series = history[column].dropna()
        if series.empty:
            continue
        base_value = float(series.iloc[0])
        if base_value == 0:
            continue
        performance[column] = (history[column] / base_value - 1) * 100
    return performance


def build_display_history(history: pd.DataFrame, smooth_closures: bool) -> pd.DataFrame:
    if not smooth_closures:
        return history

    # For visual comparison only, carry the last available quote forward so
    # market closures do not produce broken lines when assets have different calendars.
    return history.sort_index().ffill()


def format_chart_index(index: pd.Index) -> list[str]:
    if not isinstance(index, pd.DatetimeIndex):
        return [str(value) for value in index]

    has_intraday_points = any(
        getattr(value, "hour", 0) != 0 or getattr(value, "minute", 0) != 0
        for value in index
    )
    if has_intraday_points:
        return [value.strftime("%d/%m %H:%M") for value in index]
    return [value.strftime("%d/%m/%Y") for value in index]


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_movers(limit: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    if yf is None:
        return pd.DataFrame(), pd.DataFrame()

    query = yf.EquityQuery(
        "and",
        [
            yf.EquityQuery("eq", ["region", "us"]),
            yf.EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
            yf.EquityQuery("gte", ["intradaymarketcap", 20_000_000_000]),
            yf.EquityQuery("gte", ["intradayprice", 5]),
            yf.EquityQuery("gt", ["dayvolume", 100_000]),
        ],
    )

    def run_screen(sort_ascending: bool) -> pd.DataFrame:
        try:
            payload = yf.screen(query, size=60, sortField="percentchange", sortAsc=sort_ascending)
        except Exception:
            return pd.DataFrame()
        quotes = payload.get("quotes", [])
        rows = []
        for quote in quotes:
            rows.append(
                {
                    "Ticker": str(quote.get("symbol") or "").upper(),
                    "Dernier cours": quote.get("regularMarketPrice"),
                    "Variation seance (%)": quote.get("regularMarketChangePercent"),
                    "Volume": quote.get("regularMarketVolume"),
                    "Marche": quote.get("fullExchangeName") or "-",
                }
            )
        return pd.DataFrame(rows)

    gainers = run_screen(sort_ascending=False)
    losers = run_screen(sort_ascending=True)
    return gainers.head(limit), losers.head(limit)


def format_large_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    value = float(value)
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{int(value)}"


_CAP_FILTERS = {
    "Tout": (300_000_000, None),
    "Small (<2B)": (300_000_000, 2_000_000_000),
    "Mid (2-10B)": (2_000_000_000, 10_000_000_000),
    "Large (>10B)": (10_000_000_000, None),
}


@st.cache_data(ttl=900, show_spinner=False)
def fetch_stock_ideas(cap_filter: str = "Tout", limit: int = 10) -> pd.DataFrame:
    if yf is None:
        return pd.DataFrame()

    cap_min, cap_max = _CAP_FILTERS.get(cap_filter, (300_000_000, None))
    filters = [
        yf.EquityQuery("eq", ["region", "us"]),
        yf.EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
        yf.EquityQuery("gte", ["intradaymarketcap", cap_min]),
        yf.EquityQuery("gte", ["intradayprice", 5]),
        yf.EquityQuery("gt", ["dayvolume", 200_000]),
    ]
    if cap_max is not None:
        filters.append(yf.EquityQuery("lte", ["intradaymarketcap", cap_max]))

    try:
        payload = yf.screen(yf.EquityQuery("and", filters), size=150, sortField="percentchange", sortAsc=False)
    except Exception:
        return pd.DataFrame()

    excluded_name_pattern = re.compile(
        r"Warrant|Rights?|Units?|Preferred|Depositary|Trust Preferred|Acquisition Corp",
        re.IGNORECASE,
    )
    rows = []
    for quote in payload.get("quotes", []):
        ticker = str(quote.get("symbol") or "").upper()
        name = str(quote.get("shortName") or quote.get("longName") or ticker)
        if not ticker or excluded_name_pattern.search(name):
            continue

        raw = pd.to_numeric(
            pd.Series({
                "price": quote.get("regularMarketPrice") or quote.get("intradayprice"),
                "day_change": quote.get("regularMarketChangePercent") or quote.get("percentchange"),
                "market_cap": quote.get("marketCap") or quote.get("intradaymarketcap"),
                "volume": quote.get("regularMarketVolume") or quote.get("dayvolume"),
                "avg_volume": quote.get("averageDailyVolume3Month") or quote.get("averageDailyVolume10Day"),
                "fifty_day": quote.get("fiftyDayAverage"),
                "two_hundred_day": quote.get("twoHundredDayAverage"),
                "week_high": quote.get("fiftyTwoWeekHigh"),
                "week_low": quote.get("fiftyTwoWeekLow"),
            }),
            errors="coerce",
        )
        price, day_change, market_cap, volume = raw["price"], raw["day_change"], raw["market_cap"], raw["volume"]
        avg_volume = raw["avg_volume"]
        fifty_day, two_hundred_day = raw["fifty_day"], raw["two_hundred_day"]
        week_high, week_low = raw["week_high"], raw["week_low"]

        if pd.isna(price) or pd.isna(market_cap) or pd.isna(day_change):
            continue

        reasons = []
        score = 0.0

        if not pd.isna(day_change) and day_change > 0:
            score += min(float(day_change), 8.0) * 0.4
            reasons.append("momentum positif")
        if not pd.isna(fifty_day) and price > fifty_day:
            score += 1.6
            reasons.append("au-dessus MA50")
        if not pd.isna(fifty_day) and not pd.isna(two_hundred_day) and fifty_day > two_hundred_day:
            score += 2.0
            reasons.append("tendance 50j>200j")
        if not pd.isna(week_high) and week_high > 0 and price >= week_high * 0.90:
            score += 1.5
            reasons.append("proche sommet 52s")
        if not pd.isna(week_low) and week_low > 0 and price >= week_low * 1.30:
            score += 0.6
            reasons.append("rebond confirme")
        if not pd.isna(volume) and not pd.isna(avg_volume) and avg_volume > 0 and volume >= avg_volume * 1.5:
            score += 1.2
            reasons.append("volume inhabituel")
        elif not pd.isna(volume) and volume >= 1_000_000:
            score += 0.8
            reasons.append("bonne liquidite")

        rows.append({
            "Nom": name,
            "Ticker": ticker,
            "Cours": round(float(price), 2),
            "Variation (%)": round(float(day_change), 2),
            "Capitalisation": format_money(market_cap),
            "Volume": format_large_number(volume),
            "Score": round(min(score, 10.0), 1),
            "Signaux": ", ".join(reasons[:3]) or "-",
            "_market_cap_raw": float(market_cap),
        })

    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    return frame.sort_values(["Score", "Variation (%)"], ascending=[False, False]).head(limit).reset_index(drop=True)


def fetch_midcap_recommendations(limit: int = 8) -> pd.DataFrame:
    return fetch_stock_ideas("Mid (2-10B)", limit)


def _as_float(value, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _as_bool(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"oui", "true", "1", "yes"}
    return bool(value)


def _text_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    text = str(value).strip()
    if not text or text == "-":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def recommendation_signal_label(row: pd.Series) -> str:
    confirmed = _as_bool(row.get("Confirmed"))
    score = _as_float(row.get("Score"))
    age_penalty = _as_float(row.get("Age_Penalty"))
    signal_age = _as_float(row.get("Signal_Age_Minutes"))
    new_observation = _as_bool(row.get("new_observation"))
    consecutive_hits = int(_as_float(row.get("Consecutive_Hits")))

    if age_penalty >= 0.8 or signal_age > 1440:
        return "Trop ancien"
    if confirmed:
        return "Confirmé"
    if new_observation and consecutive_hits <= 1 and score >= 4.0:
        return "Récent"
    if score >= 4.2:
        return "À surveiller"
    return "Faible"


def recommendation_opportunity_label(row: pd.Series) -> str:
    timing = _as_float(row.get("Opportunity_Adjustment"))
    score = _as_float(row.get("Score"))
    rsi = _as_float(row.get("RSI"), default=50.0)
    distance = _as_float(row.get("Distance_MA20 (%)"))

    if timing >= 1.0 and score >= 4.8 and rsi <= 70 and distance <= 10:
        return "Très bonne"
    if timing >= 0 and score >= 4.2 and rsi <= 75 and distance <= 15:
        return "Correcte"
    if timing > -1.0:
        return "Moyenne"
    return "Faible"


def recommendation_risk_label(row: pd.Series) -> str:
    flags = " ".join(_text_list(row.get("risk_flags"))).lower()
    rsi = _as_float(row.get("RSI"), default=50.0)
    distance = _as_float(row.get("Distance_MA20 (%)"))
    age_penalty = _as_float(row.get("Age_Penalty"))
    high_terms = ("etire", "très eleve", "tres eleve", "earnings dans 0 a 3 jours", "breakout non confirme")
    medium_terms = ("rsi", "extension", "earnings", "ma20", "relative")

    if any(term in flags for term in high_terms) or rsi > 75 or distance > 15 or age_penalty >= 0.8:
        return "Élevé"
    if flags or any(term in flags for term in medium_terms) or rsi > 70 or distance > 10 or age_penalty > 0:
        return "Moyen"
    return "Faible"


def recommendation_verdict(row: pd.Series) -> str:
    confirmed = _as_bool(row.get("Confirmed"))
    signal = row.get("Signal") or recommendation_signal_label(row)
    opportunity = row.get("Opportunité") or recommendation_opportunity_label(row)
    risk = row.get("Risque") or recommendation_risk_label(row)
    rsi = _as_float(row.get("RSI"), default=50.0)
    distance = _as_float(row.get("Distance_MA20 (%)"))
    flags = " ".join(_text_list(row.get("risk_flags"))).lower()

    too_stretched = rsi > 75 or distance > 15 or "etire" in flags or "extension" in flags
    if too_stretched and confirmed:
        return "Confirmé mais tendu"
    if too_stretched:
        return "Trop tendu"
    if confirmed and opportunity in {"Très bonne", "Correcte"} and risk != "Élevé":
        return "Meilleure opportunité"
    if signal in {"Récent", "À surveiller"} or (_as_float(row.get("Score")) >= 4.2 and not confirmed):
        return "À surveiller"
    return "Écarter"


def build_recommendation_display_frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ("why_selected", "risk_flags"):
        if col in df.columns:
            df[col] = df[col].apply(lambda values: ", ".join(values) if isinstance(values, list) else (values or "-"))
    df["Action"] = df.apply(lambda row: f"{row.get('Nom', '-') } ({row.get('Ticker', '-')})", axis=1)
    df["Signal"] = df.apply(recommendation_signal_label, axis=1)
    df["Opportunité"] = df.apply(recommendation_opportunity_label, axis=1)
    df["Risque"] = df.apply(recommendation_risk_label, axis=1)
    df["Verdict"] = df.apply(recommendation_verdict, axis=1)
    df["Pourquoi"] = df.apply(
        lambda row: ", ".join(_text_list(row.get("why_selected") or row.get("Signaux"))[:3]) or "-",
        axis=1,
    )
    verdict_rank = {
        "Meilleure opportunité": 0,
        "Confirmé mais tendu": 1,
        "À surveiller": 2,
        "Trop tendu": 3,
        "Écarter": 4,
    }
    opportunity_rank = {"Très bonne": 0, "Correcte": 1, "Moyenne": 2, "Faible": 3}
    risk_rank = {"Faible": 0, "Moyen": 1, "Élevé": 2}
    df["_verdict_rank"] = df["Verdict"].map(verdict_rank).fillna(9)
    df["_opportunity_rank"] = df["Opportunité"].map(opportunity_rank).fillna(9)
    df["_risk_rank"] = df["Risque"].map(risk_rank).fillna(9)
    df["_confirmed_sort"] = df.get("Confirmed", False).apply(lambda value: 0 if _as_bool(value) else 1)
    df["_score_sort"] = df.get("Score", 0).apply(_as_float)
    return df.sort_values(
        ["_verdict_rank", "_confirmed_sort", "_opportunity_rank", "_risk_rank", "_score_sort"],
        ascending=[True, True, True, True, False],
    ).reset_index(drop=True)


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stock_news_headlines(ticker: str) -> list[str]:
    if yf is None:
        return []
    try:
        news = yf.Ticker(ticker).news or []
        return [item.get("content", {}).get("title") or item.get("title") or "" for item in news[:5] if item]
    except Exception:
        return []


def analyze_stocks_with_ai(rows: list[dict], api_key: str) -> dict:
    import json as _json

    def _text_value(value) -> str:
        if isinstance(value, list):
            return ", ".join(str(item) for item in value if item)
        if value is None:
            return "-"
        return str(value) or "-"

    lines = []
    for r in rows:
        headlines = fetch_stock_news_headlines(r["Ticker"])
        actu = " | ".join(h for h in headlines if h) or "Aucune actu disponible"
        signals = _text_value(r.get("Signaux") or r.get("why_selected") or r.get("Pourquoi"))
        risks = _text_value(r.get("risk_flags") or r.get("Risques"))
        setup = _text_value(r.get("Setup_Type") or r.get("Setup"))
        stability = _text_value(r.get("Stability_Score") or r.get("Stabilite"))
        lines.append(
            f"{r['Ticker']} ({r['Nom']}) — Cap: {r['Capitalisation']} — "
            f"Variation: {r['Variation (%)']:+.1f}% — Score tech: {r['Score']}/10 — "
            f"Setup: {setup} — Stabilite: {stability} — "
            f"Signaux: {signals} — Risques: {risks}\n  Actu: {actu}"
        )

    prompt = (
        "Tu es analyste financier. Pour chaque action ci-dessous, reponds en JSON avec exactement ce format:\n"
        '{"TICKER": {"sentiment": "🟢"|"🟡"|"🔴", "catalyseur": "5 mots max", "risque": "5 mots max", "commentaire": "1 phrase courte en francais"}}\n\n'
        "Actions:\n" + "\n\n".join(lines)
    )

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3},
            timeout=30,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        start, end = content.find("{"), content.rfind("}") + 1
        return _json.loads(content[start:end]) if start != -1 else {}
    except Exception:
        return {}


def render_news_llm_actions(df: pd.DataFrame, *, engine: str) -> None:
    from news_context import fetch_yahoo_news, summarize_news_with_llm_cached

    api_key = get_openai_api_key()
    ticker_col = "Ticker" if "Ticker" in df.columns else "ticker"
    name_col = "Nom" if "Nom" in df.columns else "name"
    if ticker_col not in df.columns:
        return

    st.caption("Contexte IA a la demande : aucun appel LLM n'est lance tant que tu ne cliques pas sur un ticker.")
    rows = df.head(10).to_dict("records")
    for row in rows:
        ticker = str(row.get(ticker_col) or "").upper()
        if not ticker:
            continue
        label = row.get("context_label") or "Contexte non calcule"
        name = row.get(name_col) or ticker
        result_key = f"news_llm_result_{engine}_{ticker}"
        c1, c2, c3 = st.columns([1.2, 2.6, 1.2])
        c1.markdown(f"**{ticker}**")
        c2.caption(f"{name} · {label}")
        clicked = c3.button("Résumé IA", key=f"news_llm_btn_{engine}_{ticker}", use_container_width=True)
        if clicked:
            if not api_key:
                st.session_state[result_key] = {
                    "summary": "Cle OpenAI absente : impossible de generer le resume IA.",
                    "cached": False,
                }
            else:
                try:
                    news_items = fetch_yahoo_news(ticker, limit=5)
                    result = summarize_news_with_llm_cached(ticker, news_items, api_key)
                    st.session_state[result_key] = result
                except Exception as exc:
                    st.session_state[result_key] = {
                        "summary": f"Resume IA indisponible : {exc}",
                        "cached": False,
                    }
        result = st.session_state.get(result_key)
        if result:
            cache_note = "cache" if result.get("cached") else "nouveau"
            st.info(f"{ticker} · {cache_note} · {result.get('summary', '')}")


def render_stable_recommendations_section() -> None:
    from cache import read_cache, cache_freshness_label, cache_age_minutes

    # ---- Lecture du cache worker ----
    cached = read_cache("stock_ideas")
    cached_meta = read_cache("stock_ideas_meta")
    meta = cached_meta.get("data", {}) if cached_meta else {}
    regime_payload = meta.get("market_regime") or {}
    regime_adjustment = meta.get("market_regime_adjustment") or {}
    regime_suffix = ""
    if regime_payload.get("regime"):
        regime_suffix = (
            f" · regime {regime_payload.get('regime')}"
            f" ({regime_payload.get('score', 0)})"
        )
        if regime_adjustment.get("adjusted"):
            regime_suffix += f" · ajustements regime {regime_adjustment.get('adjusted')}"
    using_cache = cached is not None and cached.get("data")

    if using_cache:
        age = cache_age_minutes("stock_ideas") or 0
        if age < 35:
            st.caption(
                f"🟢 Worker actif — analyse mise a jour {cache_freshness_label('stock_ideas')} "
                f"· univers {meta.get('universe_date', '-')} "
                f"surveille {meta.get('monitored_universe_size') or meta.get('universe_size', '?')} "
                f"· shortlist {meta.get('scoring_shortlist_size') or meta.get('universe_size', '?')} "
                f"· scores {meta.get('scored_count', '?')} · "
                f"{meta.get('confirmed_count', 0)} signal(s) confirme(s)"
                f"{regime_suffix}"
            )
        elif age < 90:
            st.warning(f"🟡 Derniere analyse il y a {int(age)} min — le worker semble lent ou redemarrage en cours.")
        else:
            st.error(
                f"🔴 Worker arrete — donnees agees de {cache_freshness_label('stock_ideas')}. "
                f"Verifier : `systemctl status worker`"
            )
        all_rows = cached["data"]
    else:
        st.warning(
            "⚠️ Worker pas encore demarre — mode fallback actif. "
            "Les scores sont simplifies (pas d'historique, pas de Setup, pas d'Earnings). "
            "Lancer le worker : `systemctl start worker`"
        )
        with st.spinner("Chargement live (simplifie)..."):
            try:
                live_df = fetch_stock_ideas("Tout", limit=15)
                all_rows = live_df.to_dict("records") if not live_df.empty else []
            except Exception as exc:
                st.info(f"Impossible de charger les idees : {exc}")
                return

    if not all_rows:
        st.info("Aucune valeur disponible pour le moment.")
        return

    # ---- Filtres ----
    cap_limits = {
        "Tout": (0, float("inf")),
        "Small (<2B)": (0, 2e9),
        "Mid (2-10B)": (2e9, 10e9),
        "Large (>10B)": (10e9, float("inf")),
    }
    filter_col, setup_col, view_col, mode_col = st.columns([1.4, 1.4, 1.8, 1.2])
    cap_filter = filter_col.selectbox("Segment", options=list(cap_limits.keys()), index=0, key="stock_ideas_cap_filter")
    setup_filter = setup_col.selectbox(
        "Setup",
        options=["Tous", "breakout", "trend", "pullback"],
        index=0,
        key="stock_ideas_setup_filter",
    )
    view_filter = view_col.selectbox(
        "Vue",
        options=["Tout", "Meilleures opportunités", "Confirmés", "À surveiller", "Trop tendus"],
        index=0,
        key="stock_ideas_view_filter",
    )
    advanced_mode = mode_col.toggle("Mode avancé", value=False, key="stock_ideas_advanced_mode")
    cap_min, cap_max = cap_limits[cap_filter]
    rows = [r for r in all_rows if cap_min <= r.get("cap_raw", r.get("_market_cap_raw", 0)) < cap_max]
    if setup_filter != "Tous":
        rows = [r for r in rows if r.get("Setup_Type") == setup_filter]
    if not rows:
        st.info("Aucune valeur pour cette selection.")
        return

    df = build_recommendation_display_frame(rows)
    if view_filter == "Meilleures opportunités":
        df = df[df["Verdict"] == "Meilleure opportunité"]
    elif view_filter == "Confirmés":
        df = df[df["Confirmed"].apply(_as_bool)]
    elif view_filter == "À surveiller":
        df = df[df["Verdict"] == "À surveiller"]
    elif view_filter == "Trop tendus":
        df = df[df["Verdict"].isin(["Trop tendu", "Confirmé mais tendu"])]

    if df.empty:
        st.info("Aucune valeur pour cette selection.")
        return

    df = df.head(15).copy()
    if "Confirmed" in df.columns:
        df["Confirmed"] = df["Confirmed"].map(lambda value: "Oui" if _as_bool(value) else "Non")
    if "new_observation" in df.columns:
        df["new_observation"] = df["new_observation"].map(lambda value: "Oui" if _as_bool(value) else "Non")

    # Colonnes enrichies si disponibles (cache worker), basiques sinon
    if using_cache and advanced_mode:
        show_cols = [
            "Display_Rank", "Nom", "Ticker", "Setup_Type", "Confirmed", "Score",
            "Opportunity_Adjustment", "Raw_Score", "Age_Penalty",
            "market_regime", "market_regime_adjustment",
            "market_region", "market_session", "new_observation", "last_market_timestamp",
            "Stability_Score", "Consecutive_Hits", "Recent_Top_Hits", "Signal_Age_Minutes",
            "Cours", "Variation (%)", "Capitalisation", "RSI", "MACD",
            "RS_SPY_1m (%)", "RS_SPY_3m (%)", "Distance_MA20 (%)", "Earnings",
            "context_label", "why_selected", "risk_flags",
        ]
    elif using_cache:
        show_cols = ["Display_Rank", "Action", "Setup_Type", "Signal", "Opportunité", "Variation (%)", "Risque", "context_label", "Pourquoi", "Verdict"]
    else:
        show_cols = ["Action", "Variation (%)", "Score", "Signal", "Pourquoi"]

    display_cols = [c for c in show_cols if c in df.columns]

    st.dataframe(
        df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "Display_Rank": st.column_config.NumberColumn("#", format="%d"),
            "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=10, format="%.1f"),
            "Raw_Score": st.column_config.NumberColumn("Score brut", format="%.1f"),
            "Opportunity_Adjustment": st.column_config.NumberColumn("Timing", format="%+.1f"),
            "Age_Penalty": st.column_config.NumberColumn("Malus age", format="%.2f"),
            "Stability_Score": st.column_config.ProgressColumn("Stabilite", min_value=0, max_value=100, format="%.0f"),
            "Consecutive_Hits": st.column_config.NumberColumn("Cycles", format="%d"),
            "Signal_Age_Minutes": st.column_config.NumberColumn("Age signal (min)", format="%.0f"),
            "Variation (%)": st.column_config.NumberColumn("Variation (%)", format="%+.2f%%"),
            "RS_SPY_1m (%)": st.column_config.NumberColumn("RS/SPY 1m", format="%+.1f%%"),
            "Distance_MA20 (%)": st.column_config.NumberColumn("Distance MA20", format="%+.1f%%"),
            "RSI": st.column_config.NumberColumn("RSI", format="%.0f"),
            "Setup_Type": st.column_config.TextColumn("Setup"),
            "Confirmed": st.column_config.TextColumn("Confirme"),
            "market_regime": st.column_config.TextColumn("Regime"),
            "market_regime_adjustment": st.column_config.NumberColumn("Ajust. regime", format="%+.2f"),
            "market_region": st.column_config.TextColumn("Region"),
            "market_session": st.column_config.TextColumn("Session"),
            "new_observation": st.column_config.TextColumn("Nouv. obs."),
            "last_market_timestamp": st.column_config.TextColumn("Derniere bougie"),
            "context_label": st.column_config.TextColumn("Contexte news"),
            "why_selected": st.column_config.TextColumn("Pourquoi"),
            "risk_flags": st.column_config.TextColumn("Risques"),
            "Action": st.column_config.TextColumn("Action"),
            "Signal": st.column_config.TextColumn("Signal"),
            "Opportunité": st.column_config.TextColumn("Opportunité"),
            "Risque": st.column_config.TextColumn("Risque"),
            "Pourquoi": st.column_config.TextColumn("Pourquoi"),
            "Verdict": st.column_config.TextColumn("Verdict"),
        },
    )

    if using_cache and meta:
        setup_counts = meta.get("setup_counts") or {}
        region_counts = meta.get("region_counts") or {}
        monitored_regions = meta.get("monitored_region_counts") or {}
        st.caption(
            "Repartition setups : "
            + ", ".join(f"{name}: {count}" for name, count in setup_counts.items())
            + (
                " · regions : " + ", ".join(f"{name}: {count}" for name, count in region_counts.items())
                if region_counts else ""
            )
            + (
                " · surveilles : " + ", ".join(f"{name}: {count}" for name, count in monitored_regions.items())
                if monitored_regions else ""
            )
            + f" · seuil confirmation {meta.get('confirm_threshold', '-')}/10 sur {meta.get('confirm_cycles', '-')} cycles"
            + f" · nouvelles observations {meta.get('new_observation_count', '-')}"
            + " · cycles confirmes uniquement sur nouvelle observation de marche"
        )
        if regime_payload.get("reasons"):
            st.caption("Regime marche : " + ", ".join(str(reason) for reason in regime_payload.get("reasons", [])[:6]))

    render_news_llm_actions(df, engine="standard")


def build_smallcap_display_frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["Action"] = df.apply(
        lambda row: f"{row.get('name', '-') } ({row.get('ticker', '-')})",
        axis=1,
    )
    df["Tags"] = df.get("tags", pd.Series([[]] * len(df))).apply(
        lambda values: ", ".join(values) if isinstance(values, list) and values else "-"
    )
    df["Market Cap"] = df["market_cap"].map(format_money) if "market_cap" in df.columns else "-"
    df["Volume affiche"] = df["volume"].map(format_large_number) if "volume" in df.columns else "-"
    df["Avg vol 20j"] = df["avg_volume_20d"].map(format_large_number) if "avg_volume_20d" in df.columns else "-"
    if "smallcap_news_display" not in df.columns:
        df["smallcap_news_display"] = df.get("smallcap_news_label", pd.Series(["-"] * len(df)))
    return df


def render_smallcap_opportunities_section() -> None:
    from cache import read_cache, cache_freshness_label, cache_age_minutes

    cached = read_cache("smallcap_ideas")
    cached_meta = read_cache("smallcap_ideas_meta")
    meta = cached_meta.get("data", {}) if cached_meta else {}
    regime_payload = meta.get("market_regime") or {}
    regime_adjustment = meta.get("market_regime_adjustment") or {}
    regime_suffix = ""
    if regime_payload.get("regime"):
        regime_suffix = (
            f" · regime {regime_payload.get('regime')}"
            f" ({regime_payload.get('score', 0)})"
        )
        if regime_adjustment.get("adjusted"):
            regime_suffix += f" · malus regime {regime_adjustment.get('adjusted')}"

    if not cached or not cached.get("data"):
        st.info(
            "Aucune small cap explosive en cache pour le moment. "
            "Le worker remplira `data/cache/smallcap_ideas.json` au prochain passage."
        )
        return

    age = cache_age_minutes("smallcap_ideas") or 0
    if age < 30:
        st.caption(
            f"Scanner small caps actif — mise a jour {cache_freshness_label('smallcap_ideas')} "
            f"· candidats {meta.get('candidate_count', '?')} "
            f"· scores {meta.get('scored_count', '?')} "
            f"· duree {meta.get('duration_seconds', '?')}s"
            f"{regime_suffix}"
        )
    elif age < 90:
        st.warning(f"Dernier scan small caps il y a {int(age)} min — donnees a rafraichir.")
    else:
        st.error(
            f"Scan small caps ancien ({cache_freshness_label('smallcap_ideas')}). "
            "Verifier le worker si cela persiste."
        )

    rows = cached.get("data") or []
    df = build_smallcap_display_frame(rows)
    if df.empty:
        st.info("Aucune small cap explosive exploitable pour le moment.")
        return

    risk_options = ["Tous"] + sorted(str(value) for value in df.get("risk", pd.Series(dtype=str)).dropna().unique())
    setup_options = ["Tous"] + sorted(str(value) for value in df.get("setup", pd.Series(dtype=str)).dropna().unique())
    tag_options = ["Tous", "first_move", "continuation", "overextended", "news_candidate", "gap_up"]

    score_col, risk_col, setup_col, tag_col = st.columns([1.1, 1.2, 1.5, 1.3])
    min_score = score_col.slider(
        "Score min",
        min_value=0.0,
        max_value=10.0,
        value=4.0,
        step=0.5,
        key="smallcap_min_score",
    )
    risk_filter = risk_col.selectbox("Risque", risk_options, index=0, key="smallcap_risk_filter")
    setup_filter = setup_col.selectbox("Setup", setup_options, index=0, key="smallcap_setup_filter")
    tag_filter = tag_col.selectbox("Tag", tag_options, index=0, key="smallcap_tag_filter")

    df = df[df["Explosion_Score"].apply(_as_float) >= min_score]
    if risk_filter != "Tous":
        df = df[df["risk"] == risk_filter]
    if setup_filter != "Tous":
        df = df[df["setup"] == setup_filter]
    if tag_filter != "Tous":
        df = df[df["Tags"].str.contains(tag_filter, na=False)]

    if df.empty:
        st.info("Aucune small cap pour cette selection.")
        return

    show_cols = [
        "rank", "Action", "price", "change_pct", "rel_volume", "Explosion_Score",
        "setup", "risk", "smallcap_news_display", "market_regime_adjustment",
        "smallcap_news_summary", "comment", "Tags", "last_market_timestamp",
    ]
    advanced = st.toggle("Details techniques small caps", value=False, key="smallcap_advanced")
    if advanced:
        show_cols = [
            "rank", "ticker", "name", "price", "change_pct", "Market Cap", "Volume affiche",
            "Avg vol 20j", "rel_volume", "Explosion_Score", "rsi_14",
            "distance_from_ma20_pct", "close_vs_day_high", "volatility",
            "setup", "risk", "comment", "Tags", "market_session", "last_market_timestamp",
            "last_observed_price", "context_label", "smallcap_news_label", "smallcap_news_display",
            "smallcap_news_summary", "smallcap_news_adjustment", "market_regime", "market_regime_adjustment",
        ]
    display_cols = [col for col in show_cols if col in df.columns]

    st.dataframe(
        df[display_cols].head(30),
        use_container_width=True,
        hide_index=True,
        column_config={
            "rank": st.column_config.NumberColumn("#", format="%d"),
            "Action": st.column_config.TextColumn("Action"),
            "ticker": st.column_config.TextColumn("Ticker"),
            "name": st.column_config.TextColumn("Nom"),
            "price": st.column_config.NumberColumn("Prix", format="%.2f"),
            "change_pct": st.column_config.NumberColumn("Variation", format="%+.2f%%"),
            "rel_volume": st.column_config.NumberColumn("Volume relatif", format="x%.2f"),
            "Explosion_Score": st.column_config.ProgressColumn("Explosion", min_value=0, max_value=10, format="%.1f"),
            "setup": st.column_config.TextColumn("Setup"),
            "risk": st.column_config.TextColumn("Risque"),
            "context_label": st.column_config.TextColumn("Contexte news"),
            "smallcap_news_label": st.column_config.TextColumn("Label news"),
            "smallcap_news_display": st.column_config.TextColumn("News small cap"),
            "smallcap_news_summary": st.column_config.TextColumn("Resume news"),
            "smallcap_news_adjustment": st.column_config.NumberColumn("Ajust. news", format="%+.2f"),
            "market_regime": st.column_config.TextColumn("Regime"),
            "market_regime_adjustment": st.column_config.NumberColumn("Ajust. regime", format="%+.2f"),
            "comment": st.column_config.TextColumn("Commentaire"),
            "Tags": st.column_config.TextColumn("Tags"),
            "last_market_timestamp": st.column_config.TextColumn("Derniere bougie"),
            "Market Cap": st.column_config.TextColumn("Capitalisation"),
            "Volume affiche": st.column_config.TextColumn("Volume"),
            "Avg vol 20j": st.column_config.TextColumn("Vol. moy. 20j"),
            "rsi_14": st.column_config.NumberColumn("RSI 14", format="%.1f"),
            "distance_from_ma20_pct": st.column_config.NumberColumn("Dist. MA20", format="%+.1f%%"),
            "close_vs_day_high": st.column_config.NumberColumn("Close / high", format="%.3f"),
            "volatility": st.column_config.NumberColumn("Volatilite", format="%.1f%%"),
            "market_session": st.column_config.TextColumn("Session"),
            "last_observed_price": st.column_config.NumberColumn("Dernier prix obs.", format="%.2f"),
        },
    )
    render_news_llm_actions(df, engine="smallcap")

    filters = meta.get("filters") or {}
    if filters:
        st.caption(
            "Univers small caps US : "
            f"prix {filters.get('min_price', '-')}-{filters.get('max_price', '-')}$ "
            f"· cap max {format_money(filters.get('max_market_cap'))} "
            f"· volume moyen min {format_large_number(filters.get('min_avg_volume'))}. "
            "Ce moteur assume RSI eleve, extension et absence de stabilite multi-cycles."
        )
    if regime_payload.get("reasons"):
        st.caption("Regime marche : " + ", ".join(str(reason) for reason in regime_payload.get("reasons", [])[:6]))


def render_midcap_recommendations_section() -> None:
    from cache import read_cache

    render_section_heading("Valeurs a fort potentiel", "Signaux classes par moteur, regime de marche et qualite du setup.")
    stable_cached = read_cache("stock_ideas") or {}
    stable_meta_cached = read_cache("stock_ideas_meta") or {}
    smallcap_cached = read_cache("smallcap_ideas") or {}
    stable_rows = stable_cached.get("data") or []
    smallcap_rows = smallcap_cached.get("data") or []
    all_scores = [
        _as_float(row.get("Score", row.get("score", row.get("Explosion_Score"))))
        for row in stable_rows + smallcap_rows
    ]
    all_scores = [score for score in all_scores if not pd.isna(score)]
    regime_payload = (stable_meta_cached.get("data") or {}).get("market_regime") or {}
    regime = regime_payload.get("regime") or "-"
    confirmed_count = sum(1 for row in stable_rows if _as_bool(row.get("Confirmed", row.get("confirmed"))))
    render_summary_strip(
        "Decision rapide",
        [
            {"label": "Signaux detectes", "value": len(stable_rows) + len(smallcap_rows), "hint": "stables + small caps", "tone": "info"},
            {"label": "Meilleur score", "value": f"{max(all_scores):.1f}/10" if all_scores else "-", "hint": "score max disponible", "tone": "success" if all_scores else "warning"},
            {"label": "Signaux confirmes", "value": confirmed_count, "hint": "validation multi-cycle", "tone": "success" if confirmed_count else "warning"},
            {"label": "Regime marche", "value": regime, "hint": f"score {regime_payload.get('score', '-')}", "tone": "danger" if regime == "RISK_OFF" else "success" if regime == "RISK_ON" else "warning"},
        ],
    )
    stable_tab, smallcap_tab = st.tabs(["Signaux stables", "Small caps explosives"])
    with stable_tab:
        st.caption("Moteur principal : signaux plus propres, confirmes et suivables.")
        render_stable_recommendations_section()
    with smallcap_tab:
        st.caption("Moteur agressif : momentum court terme, breakout, volume spike et risque eleve.")
        render_smallcap_opportunities_section()
    st.divider()
    render_signal_tracking_summary()


def render_signal_tracking_summary() -> None:
    from signal_tracking import summarize_signal_outcomes

    render_section_heading("Suivi des signaux", "Lecture objective des performances futures des recommandations detectees.")
    try:
        summary = summarize_signal_outcomes(since_days=90)
    except Exception as exc:
        st.info(f"Suivi indisponible pour le moment : {exc}")
        return

    engine_rows = summary.get("by_engine") or []
    if not engine_rows:
        st.info("Aucun signal suivi pour le moment. Le worker remplira les statistiques au fil des prochains runs.")
        return

    metrics_df = pd.DataFrame(engine_rows)
    label_map = {"standard": "Signaux stables", "smallcap": "Small caps explosives"}
    metrics_df["Moteur"] = metrics_df["engine"].map(label_map).fillna(metrics_df["engine"])
    display_cols = [
        "Moteur", "total", "complete", "avg_1d", "avg_3d", "avg_5d",
        "win_1d", "win_3d", "win_5d", "avg_runup", "avg_drawdown",
    ]
    display_cols = [col for col in display_cols if col in metrics_df.columns]
    st.dataframe(
        metrics_df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "total": st.column_config.NumberColumn("Signaux suivis", format="%d"),
            "complete": st.column_config.NumberColumn("Complets", format="%d"),
            "avg_1d": st.column_config.NumberColumn("Perf moy. J+1", format="%+.2f%%"),
            "avg_3d": st.column_config.NumberColumn("Perf moy. J+3", format="%+.2f%%"),
            "avg_5d": st.column_config.NumberColumn("Perf moy. J+5", format="%+.2f%%"),
            "win_1d": st.column_config.NumberColumn("Taux + J+1", format="%.1f%%"),
            "win_3d": st.column_config.NumberColumn("Taux + J+3", format="%.1f%%"),
            "win_5d": st.column_config.NumberColumn("Taux + J+5", format="%.1f%%"),
            "avg_runup": st.column_config.NumberColumn("Run-up moy.", format="%+.2f%%"),
            "avg_drawdown": st.column_config.NumberColumn("Drawdown moy.", format="%+.2f%%"),
        },
    )

    setup_rows = summary.get("top_setups") or []
    if setup_rows:
        setup_df = pd.DataFrame(setup_rows)
        setup_df["Moteur"] = setup_df["engine"].map(label_map).fillna(setup_df["engine"])
        st.caption("Meilleurs setups recents avec performance J+5 disponible")
        st.dataframe(
            setup_df[["Moteur", "setup", "total", "avg_5d", "win_5d", "avg_runup"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "setup": st.column_config.TextColumn("Setup"),
                "total": st.column_config.NumberColumn("Signaux", format="%d"),
                "avg_5d": st.column_config.NumberColumn("Perf moy. J+5", format="%+.2f%%"),
                "win_5d": st.column_config.NumberColumn("Taux + J+5", format="%.1f%%"),
                "avg_runup": st.column_config.NumberColumn("Run-up moy.", format="%+.2f%%"),
            },
        )


def render_market_movers_section(catalog: pd.DataFrame) -> None:
    render_section_heading(
        "A la une",
        "Un coup d'oeil rapide sur les grandes capitalisations des gros indices US, pas sur les petites valeurs speculatives.",
    )

    with st.spinner("Je charge les plus fortes variations du jour..."):
        try:
            gainers, losers = fetch_market_movers(limit=50)
        except Exception as exc:  # pragma: no cover - depends on network/provider
            st.info(f"Impossible de charger les variations du jour : {exc}")
            return

    companies = catalog[catalog["asset_type"] == "Entreprise"][["ticker", "name", "exchange"]].drop_duplicates(subset=["ticker"])
    excluded_name_pattern = r"Warrant|Rights?|Units?|Preferred|Depositary|Trust Preferred"
    companies = companies[~companies["name"].astype(str).str.contains(excluded_name_pattern, case=False, na=False)]
    companies = companies.rename(columns={"ticker": "Ticker", "name": "Nom", "exchange": "Marche catalogue"})

    def prepare(frame: pd.DataFrame, ascending: bool) -> pd.DataFrame:
        if frame.empty:
            return frame
        merged = frame.merge(companies, on="Ticker", how="inner")
        if merged.empty:
            return merged
        merged["Dernier cours"] = pd.to_numeric(merged["Dernier cours"], errors="coerce").round(2)
        merged["Variation seance (%)"] = pd.to_numeric(merged["Variation seance (%)"], errors="coerce").round(2)
        merged["Volume"] = pd.to_numeric(merged["Volume"], errors="coerce")
        merged = merged.dropna(subset=["Variation seance (%)", "Dernier cours"])
        merged = merged.sort_values("Variation seance (%)", ascending=ascending)
        return merged[["Nom", "Ticker", "Marche", "Dernier cours", "Variation seance (%)", "Volume"]].head(5)

    gainers_table = prepare(gainers, ascending=False)
    losers_table = prepare(losers, ascending=True)

    if gainers_table.empty and losers_table.empty:
        st.info("Aucune variation exploitable pour le moment.")
        return

    def format_volume(value: float) -> str:
        if pd.isna(value):
            return "-"
        value = float(value)
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.1f}B"
        if value >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        if value >= 1_000:
            return f"{value / 1_000:.1f}K"
        return f"{int(value)}"

    def render_card(title: str, frame: pd.DataFrame, accent: str, background: str) -> None:
        if frame.empty:
            st.info("Aucune variation exploitable pour le moment.")
            return

        rows_markup = []
        for _, row in frame.head(5).iterrows():
            rows_markup.append(
                f"""
                <div style="
                    padding:12px 0;
                    border-top:1px solid rgba(148, 163, 184, 0.18);
                ">
                    <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;">
                        <div>
                            <div style="font-size:1rem;font-weight:800;color:#0f172a;">
                                {escape(str(row["Nom"]))}
                            </div>
                            <div style="margin-top:2px;font-size:0.88rem;color:#475569;">
                                {escape(str(row["Ticker"]))} · {escape(str(row["Marche"]))}
                            </div>
                            <div style="margin-top:6px;font-size:0.85rem;color:#64748b;">
                                Cours : {float(row["Dernier cours"]):.2f} · Volume : {format_volume(row["Volume"])}
                            </div>
                        </div>
                        <div style="font-size:1.15rem;font-weight:900;color:{accent};white-space:nowrap;">
                            {float(row["Variation seance (%)"]):+.2f}%
                        </div>
                    </div>
                </div>
                """
            )

        st.html(
            f"""
            <div style="
                background:{background};
                border:1px solid {accent};
                border-radius:8px;
                padding:16px 18px;
                box-shadow:0 8px 22px rgba(15, 23, 42, 0.06);
                min-height:360px;
            ">
                <div style="font-size:0.8rem;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:{accent};">
                    {escape(title)}
                </div>
                <div style="margin-top:10px;font-size:0.96rem;color:#334155;">
                    Les cinq plus gros mouvements du jour.
                </div>
                <div style="margin-top:14px;">
                    {''.join(rows_markup)}
                </div>
            </div>
            """
        )

    gainers_col, losers_col = st.columns(2)
    with gainers_col:
        render_card("Ca monte fort", gainers_table, "#15803d", "#f0fdf4")

    with losers_col:
        render_card("Ca baisse fort", losers_table, "#b91c1c", "#fef2f2")


def format_news_datetime(value: str | None) -> str:
    if not value:
        return "-"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    local_value = parsed.astimezone(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    return local_value


def parse_iso_datetime_value(value: str | None) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def format_rss_datetime(value: str | None) -> str:
    if not value:
        return "-"
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")


def parse_rss_datetime_value(value: str | None) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return datetime.fromtimestamp(0, tz=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_news_for_tickers(tickers: tuple[str, ...], per_ticker_limit: int = 8) -> list[dict]:
    if yf is None:
        return []

    news_items: list[dict] = []
    seen_ids: set[str] = set()

    for ticker in tickers:
        try:
            ticker_news = yf.Ticker(ticker).news or []
        except Exception:
            ticker_news = []

        for raw_item in ticker_news[:per_ticker_limit]:
            content = raw_item.get("content", {})
            item_id = str(raw_item.get("id") or content.get("id") or "")
            if item_id and item_id in seen_ids:
                continue
            if item_id:
                seen_ids.add(item_id)

            news_items.append(
                {
                    "id": item_id,
                    "ticker": ticker,
                    "title": content.get("title", "Sans titre"),
                    "summary": content.get("summary") or content.get("description") or "",
                    "provider": (content.get("provider") or {}).get("displayName", "Source inconnue"),
                    "published_at": content.get("pubDate") or content.get("displayTime"),
                    "url": (content.get("clickThroughUrl") or {}).get("url")
                    or (content.get("canonicalUrl") or {}).get("url"),
                }
            )

    news_items.sort(key=lambda item: item.get("published_at") or "", reverse=True)
    return news_items


@st.cache_data(ttl=300, show_spinner=False)
def fetch_general_news(category: str) -> list[dict]:
    feeds = GENERAL_NEWS_FEEDS.get(category, [])
    headers = {"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml"}
    news_items: list[dict] = []
    seen_links: set[str] = set()

    for feed in feeds:
        try:
            response = requests.get(feed["url"], headers=headers, timeout=20)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception:
            continue

        rss_items = root.findall("./channel/item")
        atom_items = root.findall("{http://www.w3.org/2005/Atom}entry")
        for item in (rss_items or atom_items)[:12]:
            is_atom = item.tag.endswith("entry")
            title = (
                item.findtext("{http://www.w3.org/2005/Atom}title")
                if is_atom
                else item.findtext("title")
            ) or ""
            description = (
                item.findtext("{http://www.w3.org/2005/Atom}summary")
                or item.findtext("{http://www.w3.org/2005/Atom}content")
                if is_atom
                else item.findtext("description")
            ) or ""
            pub_date = (
                item.findtext("{http://www.w3.org/2005/Atom}updated")
                or item.findtext("{http://www.w3.org/2005/Atom}published")
                if is_atom
                else item.findtext("pubDate")
            ) or ""
            if is_atom:
                link_element = item.find("{http://www.w3.org/2005/Atom}link")
                link = (link_element.attrib.get("href", "") if link_element is not None else "").strip()
                source = feed["label"]
            else:
                link = (item.findtext("link") or "").strip()
                source = (item.findtext("source") or feed["label"]).strip()
            title = title.strip()
            description = description.strip()
            pub_date = pub_date.strip()
            image_url = extract_first_image_url(description)
            if not title or not link or link in seen_links:
                continue
            seen_links.add(link)
            news_items.append(
                {
                    "title": title,
                    "url": link,
                    "summary": description,
                    "source": source,
                    "feed_label": feed["label"],
                    "feed_url": feed["url"],
                    "published_at": pub_date,
                    "image_url": image_url,
                }
            )

    def sort_key(item: dict) -> float:
        try:
            return parsedate_to_datetime(item.get("published_at", "")).timestamp()
        except Exception:
            return 0.0

    news_items.sort(key=sort_key, reverse=True)
    return news_items


def sort_market_news_items(news_items: list[dict], sort_by: str) -> list[dict]:
    if sort_by == "Plus anciennes":
        return sorted(news_items, key=lambda item: parse_iso_datetime_value(item.get("published_at")))
    if sort_by == "Source A-Z":
        return sorted(news_items, key=lambda item: ((item.get("provider") or "").lower(), (item.get("title") or "").lower()))
    if sort_by == "Actif A-Z":
        return sorted(news_items, key=lambda item: ((item.get("ticker") or "").lower(), (item.get("title") or "").lower()))
    if sort_by == "Titre A-Z":
        return sorted(news_items, key=lambda item: (item.get("title") or "").lower())
    return sorted(news_items, key=lambda item: parse_iso_datetime_value(item.get("published_at")), reverse=True)


def sort_general_news_items(news_items: list[dict], sort_by: str) -> list[dict]:
    if sort_by == "Plus anciennes":
        return sorted(news_items, key=lambda item: parse_rss_datetime_value(item.get("published_at")))
    if sort_by == "Source A-Z":
        return sorted(news_items, key=lambda item: ((item.get("source") or "").lower(), (item.get("title") or "").lower()))
    if sort_by == "Flux A-Z":
        return sorted(news_items, key=lambda item: ((item.get("feed_label") or "").lower(), (item.get("title") or "").lower()))
    if sort_by == "Titre A-Z":
        return sorted(news_items, key=lambda item: (item.get("title") or "").lower())
    return sorted(news_items, key=lambda item: parse_rss_datetime_value(item.get("published_at")), reverse=True)


def extract_first_image_url(value: str) -> str:
    if not value:
        return ""
    match = re.search(r"<img[^>]+src=[\"']([^\"']+)[\"']", value, flags=re.IGNORECASE)
    if not match:
        return ""
    return unescape(match.group(1)).strip()


def clean_summary_text(value: str, max_length: int = 240) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    text = re.sub(r"\s+", " ", unescape(text)).strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def render_general_news_card(item: dict) -> None:
    image_url = item.get("image_url") or extract_first_image_url(item.get("summary") or "")
    title = clean_summary_text(item.get("title") or "Sans titre", max_length=130)
    summary = clean_summary_text(item.get("summary") or "", max_length=260)
    source = item.get("source") or "Source inconnue"
    feed_label = item.get("feed_label") or "Flux inconnu"
    published = format_rss_datetime(item.get("published_at"))
    url = item.get("url") or ""

    st.markdown(
        """
        <div class="news-card"></div>
        """,
        unsafe_allow_html=True,
    )
    image_col, text_col = st.columns([1, 2.4], vertical_alignment="top")
    with image_col:
        if image_url:
            try:
                st.image(image_url, width="stretch")
            except Exception:
                st.markdown(
                    """
                    <div style="display:flex;align-items:center;justify-content:center;min-height:180px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;color:#64748b;font-size:0.92rem;">
                        Image indisponible
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                """
                <div style="display:flex;align-items:center;justify-content:center;min-height:180px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;color:#64748b;font-size:0.92rem;">
                    Image indisponible
                </div>
                """,
                unsafe_allow_html=True,
            )
    with text_col:
        if url:
            st.markdown(f"<p class='news-card-title'><a href='{escape(url)}'>{escape(title)}</a></p>", unsafe_allow_html=True)
        else:
            st.markdown(f"<p class='news-card-title'>{escape(title)}</p>", unsafe_allow_html=True)
        st.markdown(
            f"<div class='news-card-meta'>{escape(source)} | {escape(feed_label)} | {escape(published)}</div>",
            unsafe_allow_html=True,
        )
        if summary:
            st.markdown(f"<p class='news-card-summary'>{escape(summary)}</p>", unsafe_allow_html=True)


def split_email_recipients(value: str) -> list[str]:
    recipients = []
    for part in re.split(r"[,;\n]+", value or ""):
        candidate = part.strip()
        if candidate:
            recipients.append(candidate)
    return recipients


def merge_email_recipient_values(*values: str) -> str:
    recipients: list[str] = []
    seen: set[str] = set()
    for value in values:
        for recipient in split_email_recipients(value):
            normalized = recipient.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            recipients.append(recipient)
    return ", ".join(recipients)


def get_news_email_config(
    recipients_override: str | None = None,
    sender_override: str | None = None,
    username_override: str | None = None,
    password_override: str | None = None,
) -> dict:
    sender = (sender_override or os.getenv("NEWS_EMAIL_FROM") or os.getenv("NEWS_SMTP_USER") or os.getenv("GMAIL_USER") or "").strip()
    username = (username_override or os.getenv("NEWS_SMTP_USER") or os.getenv("GMAIL_USER") or sender).strip()
    password = (
        password_override or os.getenv("NEWS_SMTP_PASSWORD") or os.getenv("NEWS_EMAIL_PASSWORD") or os.getenv("GMAIL_PASSWORD") or ""
    ).strip()
    recipients_source = (
        recipients_override
        if recipients_override is not None
        else os.getenv("NEWS_EMAIL_TO") or os.getenv("NEWS_EMAIL_RECIPIENTS") or ""
    )
    host = os.getenv("NEWS_SMTP_HOST", "smtp.gmail.com").strip() or "smtp.gmail.com"
    if host == "smtp.gmail.com":
        password = re.sub(r"\s+", "", password)
    try:
        port = int(os.getenv("NEWS_SMTP_PORT", "465"))
    except ValueError:
        port = 465
    return {
        "host": host,
        "port": port,
        "sender": sender,
        "username": username,
        "password": password,
        "recipients": split_email_recipients(recipients_source),
    }


def validate_news_email_config(config: dict) -> list[str]:
    errors = []
    if not config.get("sender"):
        errors.append("NEWS_EMAIL_FROM ou NEWS_SMTP_USER est manquant.")
    if not config.get("username"):
        errors.append("NEWS_SMTP_USER est manquant.")
    if not config.get("password"):
        errors.append("NEWS_SMTP_PASSWORD est manquant.")
    recipients = config.get("recipients") or []
    if not recipients:
        errors.append("NEWS_EMAIL_TO est manquant.")
    invalid_recipients = [recipient for recipient in recipients if not EMAIL_PATTERN.fullmatch(recipient)]
    if invalid_recipients:
        errors.append(f"Destinataire invalide : {', '.join(invalid_recipients)}.")
    return errors


def dataframe_records(frame: pd.DataFrame, limit: int = 5) -> list[dict]:
    if frame is None or frame.empty:
        return []
    return frame.head(limit).fillna("-").to_dict("records")


def build_daily_news_recap(
    categories: list[str] | tuple[str, ...] | None = None,
    items_per_category: int = NEWS_RECAP_DEFAULT_LIMIT,
) -> dict:
    selected_categories = [
        category
        for category in (categories or NEWS_RECAP_DEFAULT_CATEGORIES)
        if category in GENERAL_NEWS_FEEDS
    ] or [NEWS_RECAP_DEFAULT_CATEGORIES[0]]
    today_label = datetime.now().strftime("%d/%m/%Y")
    generated_label = datetime.now().strftime("%d/%m/%Y %H:%M")

    general_sections = []
    for category in selected_categories:
        items = sort_general_news_items(fetch_general_news(category), "Plus recentes")[:items_per_category]
        general_sections.append({"category": category, "items": items})

    try:
        gainers, losers = fetch_market_movers(limit=5)
    except Exception:
        gainers, losers = pd.DataFrame(), pd.DataFrame()

    try:
        midcaps = fetch_midcap_recommendations(limit=5)
    except Exception:
        midcaps = pd.DataFrame()

    subject = f"Recap infos du {today_label}"
    text_lines = [
        subject,
        f"Genere le {generated_label}",
    ]
    html_sections = [
        "<html><body>",
        f"<h1>{escape(subject)}</h1>",
        f"<p><em>Genere le {escape(generated_label)}</em></p>",
    ]

    gainers_records = dataframe_records(gainers)
    losers_records = dataframe_records(losers)
    if gainers_records or losers_records:
        text_lines.extend(["", "Infos bourse et marche"])
        html_sections.append("<h2>Infos bourse et marche</h2>")
        for label, records in (("Ca monte", gainers_records), ("Ca baisse", losers_records)):
            if not records:
                continue
            text_lines.append(label)
            html_sections.append(f"<h3>{escape(label)}</h3><ul>")
            for row in records:
                ticker = str(row.get("Ticker") or "-")
                variation = row.get("Variation seance (%)", "-")
                price = row.get("Dernier cours", "-")
                text_lines.append(f"- {ticker} | {variation}% | cours {price}")
                html_sections.append(
                    f"<li><strong>{escape(ticker)}</strong> | {escape(str(variation))}% | cours {escape(str(price))}</li>"
                )
            html_sections.append("</ul>")

    midcap_records = dataframe_records(midcaps)
    if midcap_records:
        text_lines.extend(["", "Valeurs a fort potentiel"])
        html_sections.append("<h2>Valeurs a fort potentiel</h2><ul>")
        for row in midcap_records:
            name = str(row.get("Nom") or "-")
            ticker = str(row.get("Ticker") or "-")
            score = row.get("Score", "-")
            why = str(row.get("Pourquoi") or "")
            text_lines.append(f"- {name} ({ticker}) | score {score}/10 | {why}")
            html_sections.append(
                f"<li><strong>{escape(name)} ({escape(ticker)})</strong> | score {escape(str(score))}/10<br>{escape(why)}</li>"
            )
        html_sections.append("</ul>")

    disclaimer = "Les idees marche ne sont pas des conseils financiers."
    text_lines.extend(["", disclaimer])
    html_sections.append(f"<p><em>{escape(disclaimer)}</em></p>")

    text_lines.extend(["", "Actualites generales"])
    html_sections.append("<h2>Actualites generales</h2>")
    for section in general_sections:
        text_lines.extend(["", section["category"]])
        html_sections.append(f"<h3>{escape(section['category'])}</h3>")
        if not section["items"]:
            text_lines.append("- Aucune info chargee.")
            html_sections.append("<p>Aucune info chargee.</p>")
            continue

        html_sections.append("<ul>")
        for item in section["items"]:
            title = item.get("title") or "Sans titre"
            source = item.get("source") or item.get("feed_label") or "Source inconnue"
            published = format_rss_datetime(item.get("published_at"))
            url = item.get("url") or ""
            summary = clean_summary_text(item.get("summary") or "")
            text_lines.append(f"- {title} | {source} | {published}")
            if summary:
                text_lines.append(f"  {summary}")
            if url:
                text_lines.append(f"  {url}")
            html_sections.append(
                "<li>"
                f"<strong>{escape(title)}</strong><br>"
                f"<span>{escape(source)} | {escape(published)}</span>"
                + (f"<p>{escape(summary)}</p>" if summary else "")
                + (f"<a href=\"{escape(url)}\">Lire l'article</a>" if url else "")
                + "</li>"
            )
        html_sections.append("</ul>")

    html_sections.append("</body></html>")

    return {
        "subject": subject,
        "text": "\n".join(text_lines),
        "html": "\n".join(html_sections),
        "categories": selected_categories,
    }


def send_email_message(
    subject: str,
    text_body: str,
    html_body: str,
    recipients_override: str | None = None,
    sender_override: str | None = None,
    username_override: str | None = None,
    password_override: str | None = None,
    related_images: list[dict] | None = None,
) -> int:
    config = get_news_email_config(
        recipients_override,
        sender_override=sender_override,
        username_override=username_override,
        password_override=password_override,
    )
    errors = validate_news_email_config(config)
    if errors:
        raise ValueError("Configuration mail incomplete : " + " ".join(errors))

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config["sender"]
    message["To"] = ", ".join(config["recipients"])
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")
    if related_images:
        html_part = message.get_payload()[-1]
        for image in related_images:
            html_part.add_related(
                image["data"],
                maintype=image.get("maintype", "image"),
                subtype=image.get("subtype", "png"),
                cid=f"<{image['cid']}>",
                filename=image.get("filename", "image.png"),
            )

    with smtplib.SMTP_SSL(config["host"], config["port"], timeout=30) as server:
        server.login(config["username"], config["password"])
        server.send_message(message)
    return len(config["recipients"])


def send_daily_news_recap_email(
    recipients_override: str | None = None,
    categories: list[str] | tuple[str, ...] | None = None,
    items_per_category: int = NEWS_RECAP_DEFAULT_LIMIT,
    sender_override: str | None = None,
    username_override: str | None = None,
    password_override: str | None = None,
) -> dict:
    recap = build_daily_news_recap(categories=categories, items_per_category=items_per_category)
    sent_count = send_email_message(
        recap["subject"],
        recap["text"],
        recap["html"],
        recipients_override=recipients_override,
        sender_override=sender_override,
        username_override=username_override,
        password_override=password_override,
    )
    return {"subject": recap["subject"], "sent_count": sent_count, "categories": recap["categories"]}


def get_openai_api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def local_tts_engines() -> dict[str, str]:
    engines = {}
    espeak_ng = shutil.which("espeak-ng")
    if espeak_ng:
        engines["espeak-ng local"] = espeak_ng
    espeak = shutil.which("espeak")
    if espeak:
        engines["espeak local"] = espeak
    return engines


def ensure_briefings_dir() -> None:
    BRIEFINGS_DIR.mkdir(parents=True, exist_ok=True)


def get_briefing_output_dir(label: str | None = None) -> Path:
    ensure_briefings_dir()
    safe_label = re.sub(r"[^0-9A-Za-z_-]+", "-", label or datetime.now().strftime("%Y-%m-%d")).strip("-")
    path = BRIEFINGS_DIR / (safe_label or datetime.now().strftime("%Y-%m-%d"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_generated_podcasts(limit: int = 30) -> list[dict]:
    if not BRIEFINGS_DIR.exists():
        return []

    podcasts: list[dict] = []
    for audio_path in BRIEFINGS_DIR.glob("*/briefing.mp3"):
        folder = audio_path.parent
        try:
            modified_at = audio_path.stat().st_mtime
        except OSError:
            continue

        label = folder.name
        parsed_label = label
        try:
            parsed_label = datetime.strptime(label, "%Y-%m-%d-%H%M%S").strftime("%d/%m/%Y %H:%M")
        except ValueError:
            pass

        size_mb = 0.0
        try:
            size_mb = audio_path.stat().st_size / (1024 * 1024)
        except OSError:
            pass

        script_path = folder / "script.md"
        context_path = folder / "context.json"
        podcasts.append(
            {
                "label": parsed_label,
                "folder": label,
                "audio_path": audio_path,
                "script_path": script_path if script_path.exists() else None,
                "context_path": context_path if context_path.exists() else None,
                "modified_at": modified_at,
                "size_mb": size_mb,
            }
        )

    podcasts.sort(key=lambda item: item["modified_at"], reverse=True)
    return podcasts[:limit]


def compact_news_item(item: dict) -> dict:
    return {
        "title": clean_summary_text(item.get("title") or "", max_length=160),
        "summary": clean_summary_text(item.get("summary") or "", max_length=220),
        "source": item.get("source") or item.get("provider") or item.get("feed_label") or "Source inconnue",
        "feed": item.get("feed_label") or item.get("ticker") or "",
        "published_at": item.get("published_at") or "",
        "url": item.get("url") or "",
    }


def fetch_article_content(url: str, max_chars: int = 1800) -> str:
    if _trafilatura is None or not url:
        return ""
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=8)
        response.raise_for_status()
        content = _trafilatura.extract(
            response.text,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
        )
        if content:
            return content[:max_chars]
    except Exception:
        pass
    return ""


def enrich_news_items_with_content(items: list[dict], top_n: int = 18) -> list[dict]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    to_enrich = items[:top_n]
    rest = items[top_n:]

    enriched: list[dict | None] = [None] * len(to_enrich)
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_article_content, item.get("url") or "", 2500): i
            for i, item in enumerate(to_enrich)
        }
        for future in as_completed(futures):
            i = futures[future]
            try:
                content = future.result()
                rss_summary = to_enrich[i].get("summary") or ""
                # Use RSS summary as fallback when full content unavailable (paywall etc.)
                effective_content = content or rss_summary
                enriched[i] = {**to_enrich[i], "content": effective_content} if effective_content else to_enrich[i]
            except Exception:
                enriched[i] = to_enrich[i]

    return [item for item in enriched if item is not None] + rest


def normalize_news_title(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9 ]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", normalized).strip()


FRENCH_STOPWORDS = {
    "avec",
    "dans",
    "des",
    "du",
    "elle",
    "est",
    "les",
    "leur",
    "leurs",
    "mais",
    "pour",
    "que",
    "qui",
    "sur",
    "une",
    "aux",
    "son",
    "ses",
    "plus",
    "par",
    "pas",
    "ont",
    "son",
    "sont",
    "apres",
    "avant",
    "contre",
    "comme",
    "tout",
    "tous",
    "cette",
    "ces",
    "fait",
    "faire",
    "selon",
    "vers",
    "face",
    "dont",
    "etre",
    "ete",
    "sans",
    "apres",
    "pendant",
}


SOURCE_QUALITY_WEIGHTS = {
    "Le Monde": 1.2,
    "Les Echos": 1.2,
    "Franceinfo": 1.1,
    "France 24": 1.1,
    "RFI": 1.05,
    "Le Figaro": 1.0,
    "BFMTV": 0.9,
    "BFM Business": 1.0,
    "Challenges": 0.9,
    "20 Minutes": 0.75,
}

CATEGORY_WEIGHTS = {
    "A la une": 1.15,
    "Politique": 1.05,
    "International": 1.15,
    "Economie": 1.15,
    "France": 1.05,
    "Tech / Sciences": 0.95,
    "Culture / Societe": 0.85,
}

HIGH_IMPACT_KEYWORDS = {
    "guerre", "cessez", "attaque", "election", "president", "gouvernement",
    "budget", "inflation", "croissance", "recession", "banque", "bourse",
    "marche", "taux", "ia", "intelligence", "artificielle", "climat",
    "justice", "proces", "crise", "accord", "sanction", "europe",
    "ukraine", "russie", "chine", "etats", "unis", "israel", "iran",
}


def title_keywords(value: str) -> set[str]:
    normalized = normalize_news_title(value)
    return {
        word
        for word in normalized.split()
        if len(word) >= 4 and word not in FRENCH_STOPWORDS and not word.isdigit()
    }


def news_cluster_id(keywords: set[str], title: str) -> str:
    key_terms = sorted(keywords)[:8] or normalize_news_title(title).split()[:8]
    raw = "|".join(key_terms)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def source_quality_weight(source: str) -> float:
    label = source or ""
    for name, weight in SOURCE_QUALITY_WEIGHTS.items():
        if name.lower() in label.lower():
            return weight
    return 0.85


def article_age_hours(item: dict, now: datetime | None = None) -> float:
    reference = now or datetime.now(timezone.utc)
    published = parse_rss_datetime_value(item.get("published_at"))
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    return max((reference - published).total_seconds() / 3600, 0.0)


def freshness_points(item: dict, now: datetime | None = None) -> float:
    age = article_age_hours(item, now)
    if age <= 3:
        return 18.0
    if age <= 6:
        return 14.0
    if age <= 12:
        return 10.0
    if age <= 24:
        return 6.0
    return 2.0


def summarize_cluster_reason(cluster: dict) -> list[str]:
    reasons = []
    if cluster.get("article_count", 0) >= 3:
        reasons.append(f"{cluster.get('article_count')} articles")
    if cluster.get("source_count", 0) >= 2:
        reasons.append(f"{cluster.get('source_count')} sources")
    if cluster.get("impact_keywords"):
        reasons.append("mots-cles forts: " + ", ".join(cluster["impact_keywords"][:3]))
    if cluster.get("freshness_score", 0) >= 12:
        reasons.append("actualite recente")
    if cluster.get("continuity_bonus", 0) > 0:
        reasons.append("continuite editoriale")
    return reasons or ["score editorial stable"]


def dedupe_compact_news(items: list[dict], limit: int = 30) -> list[dict]:
    deduped: list[dict] = []
    seen: set[str] = set()
    for item in items:
        title_key = normalize_news_title(item.get("title") or "")
        if not title_key or title_key in seen:
            continue
        seen.add(title_key)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


def build_news_topic_clusters(items: list[dict], limit: int = 8) -> list[dict]:
    clusters: list[dict] = []
    for item in items:
        keywords = title_keywords((item.get("title") or "") + " " + (item.get("summary") or ""))
        if not keywords:
            continue
        best_cluster = None
        best_overlap = 0
        for cluster in clusters:
            overlap = len(keywords & cluster["keywords"])
            if overlap > best_overlap:
                best_overlap = overlap
                best_cluster = cluster
        if best_cluster is not None and best_overlap >= 2:
            best_cluster["items"].append(item)
            best_cluster["keywords"].update(keywords)
            best_cluster["sources"].add(item.get("source") or "Source inconnue")
            if item.get("category"):
                best_cluster["categories"].add(item.get("category"))
        else:
            clusters.append(
                {
                    "keywords": set(keywords),
                    "items": [item],
                    "sources": {item.get("source") or "Source inconnue"},
                    "categories": {item.get("category")} if item.get("category") else set(),
                }
            )

    scored = [score_news_topic_cluster(cluster) for cluster in clusters]
    ranked = sorted(scored, key=lambda cluster: (cluster["score"], cluster["source_count"], cluster["article_count"]), reverse=True)
    result = []
    for cluster in ranked[:limit]:
        result.append(compact_news_cluster(cluster))
    return result


def score_news_topic_cluster(cluster: dict, editorial_state: dict | None = None) -> dict:
    items = cluster.get("items", [])
    sources = cluster.get("sources", set())
    categories = {c for c in cluster.get("categories", set()) if c}
    keywords = cluster.get("keywords", set())
    first_item = items[0] if items else {}
    cluster_id = news_cluster_id(keywords, first_item.get("title") or "")

    article_points = min(len(items), 5) * 8.0
    source_points = min(len(sources), 5) * 9.0
    freshness_score = max((freshness_points(item) for item in items), default=0.0)
    quality_score = sum(source_quality_weight(source) for source in sources) / max(len(sources), 1) * 8.0
    content_score = min(sum(len(item.get("content") or item.get("summary") or "") for item in items) / 900, 12.0)
    category_score = max((CATEGORY_WEIGHTS.get(category, 0.85) for category in categories), default=0.85) * 8.0
    impact_keywords = sorted(keywords & HIGH_IMPACT_KEYWORDS)
    impact_score = min(len(impact_keywords), 5) * 4.0

    continuity_bonus = 0.0
    if editorial_state:
        previous_main = editorial_state.get("main_cluster_id")
        recent_ids = set(editorial_state.get("recent_cluster_ids") or [])
        if cluster_id == previous_main:
            continuity_bonus += 14.0
        elif cluster_id in recent_ids:
            continuity_bonus += 6.0

    score = article_points + source_points + freshness_score + quality_score + content_score + category_score + impact_score + continuity_bonus
    return {
        **cluster,
        "cluster_id": cluster_id,
        "article_count": len(items),
        "source_count": len(sources),
        "score": round(score, 1),
        "article_points": round(article_points, 1),
        "source_points": round(source_points, 1),
        "freshness_score": round(freshness_score, 1),
        "quality_score": round(quality_score, 1),
        "content_score": round(content_score, 1),
        "category_score": round(category_score, 1),
        "impact_score": round(impact_score, 1),
        "continuity_bonus": round(continuity_bonus, 1),
        "impact_keywords": impact_keywords,
    }


def compact_news_cluster(cluster: dict, max_articles: int = 2) -> dict:
    items = sorted(
        cluster.get("items", []),
        key=lambda item: (
            source_quality_weight(item.get("source") or ""),
            freshness_points(item),
            len(item.get("content") or item.get("summary") or ""),
        ),
        reverse=True,
    )
    first_item = items[0] if items else {}
    compact_items = [compact_news_item(item) for item in items[:max_articles]]
    for idx, item in enumerate(items[:max_articles]):
        if item.get("content"):
            compact_items[idx]["content"] = item.get("content", "")[:1800]
        if item.get("category"):
            compact_items[idx]["category"] = item.get("category")

    payload = {
        "cluster_id": cluster.get("cluster_id") or news_cluster_id(cluster.get("keywords", set()), first_item.get("title") or ""),
        "main_title": first_item.get("title") or "",
        "summary": clean_summary_text(first_item.get("summary") or first_item.get("content") or "", max_length=420),
        "sources": sorted(cluster.get("sources", [])),
        "source_count": len(cluster.get("sources", [])),
        "article_count": len(cluster.get("items", [])),
        "categories": sorted(c for c in cluster.get("categories", []) if c),
        "keywords": sorted(cluster.get("keywords", []))[:12],
        "impact_keywords": cluster.get("impact_keywords", []),
        "score": cluster.get("score", 0),
        "score_breakdown": {
            "articles": cluster.get("article_points", 0),
            "sources": cluster.get("source_points", 0),
            "freshness": cluster.get("freshness_score", 0),
            "source_quality": cluster.get("quality_score", 0),
            "content": cluster.get("content_score", 0),
            "category": cluster.get("category_score", 0),
            "impact": cluster.get("impact_score", 0),
            "continuity": cluster.get("continuity_bonus", 0),
        },
        "why_selected": summarize_cluster_reason(cluster),
        "related_titles": [item.get("title") for item in items[:5] if item.get("title")],
        "reference_articles": compact_items,
    }
    return payload


def build_scored_news_topic_clusters(items: list[dict], editorial_state: dict | None = None, limit: int = 12) -> list[dict]:
    raw_clusters: list[dict] = []
    for item in items:
        keywords = title_keywords((item.get("title") or "") + " " + (item.get("summary") or "") + " " + (item.get("content") or "")[:500])
        if not keywords:
            continue
        best_cluster = None
        best_overlap = 0
        for cluster in raw_clusters:
            overlap = len(keywords & cluster["keywords"])
            if overlap > best_overlap:
                best_overlap = overlap
                best_cluster = cluster
        threshold = 2 if len(keywords) <= 8 else 3
        if best_cluster is not None and best_overlap >= threshold:
            best_cluster["items"].append(item)
            best_cluster["keywords"].update(keywords)
            best_cluster["sources"].add(item.get("source") or "Source inconnue")
            if item.get("category"):
                best_cluster["categories"].add(item.get("category"))
        else:
            raw_clusters.append(
                {
                    "keywords": set(keywords),
                    "items": [item],
                    "sources": {item.get("source") or "Source inconnue"},
                    "categories": {item.get("category")} if item.get("category") else set(),
                }
            )

    scored = [score_news_topic_cluster(cluster, editorial_state=editorial_state) for cluster in raw_clusters]
    scored.sort(key=lambda cluster: (cluster["score"], cluster["source_count"], cluster["article_count"]), reverse=True)
    return [compact_news_cluster(cluster) for cluster in scored[:limit]]


def digest_categories_match(digest: dict, categories: list[str]) -> bool:
    return set(digest.get("categories") or []) == set(categories or [])


def load_editorial_state() -> dict:
    from cache import read_cache

    cached = read_cache(EDITORIAL_STATE_CACHE_KEY)
    data = cached.get("data") if cached else {}
    return data if isinstance(data, dict) else {}


def write_editorial_state(digest: dict) -> None:
    from cache import write_cache

    clusters = [digest.get("main_topic")] + list(digest.get("secondary_topics") or [])
    clusters = [cluster for cluster in clusters if cluster]
    write_cache(
        EDITORIAL_STATE_CACHE_KEY,
        {
            "digest_id": digest.get("digest_id"),
            "generated_at": digest.get("generated_at_iso"),
            "main_cluster_id": (digest.get("main_topic") or {}).get("cluster_id"),
            "main_title": (digest.get("main_topic") or {}).get("main_title"),
            "recent_cluster_ids": [cluster.get("cluster_id") for cluster in clusters if cluster.get("cluster_id")],
            "secondary_titles": [cluster.get("main_title") for cluster in digest.get("secondary_topics", []) if cluster.get("main_title")],
        },
    )


def load_recent_news_digest(categories: list[str], max_age_minutes: int = NEWS_DIGEST_REUSE_MINUTES) -> dict | None:
    from cache import cache_age_minutes, read_cache

    age = cache_age_minutes(NEWS_DIGEST_CACHE_KEY)
    cached = read_cache(NEWS_DIGEST_CACHE_KEY)
    digest = cached.get("data") if cached else None
    if not isinstance(digest, dict) or age is None:
        return None
    if age > max_age_minutes:
        return None
    if not digest_categories_match(digest, categories):
        return None
    return {**digest, "cache_age_minutes": round(age, 1), "reused": True}


def flatten_digest_reference_articles(digest: dict, limit: int = 12) -> list[dict]:
    articles: list[dict] = []
    seen_urls: set[str] = set()
    clusters = [digest.get("main_topic")] + list(digest.get("secondary_topics") or [])
    for cluster in [cluster for cluster in clusters if cluster]:
        for item in cluster.get("reference_articles", []) or []:
            url = item.get("url") or item.get("title") or ""
            if url in seen_urls:
                continue
            seen_urls.add(url)
            articles.append(item)
            if len(articles) >= limit:
                return articles
    return articles


def should_keep_previous_main(candidate_clusters: list[dict], editorial_state: dict) -> tuple[bool, str]:
    previous_id = editorial_state.get("main_cluster_id")
    if not previous_id or not candidate_clusters:
        return False, "pas de sujet principal precedent"
    current_top = candidate_clusters[0]
    previous_cluster = next((cluster for cluster in candidate_clusters if cluster.get("cluster_id") == previous_id), None)
    if not previous_cluster:
        return False, "ancien sujet absent des flux recents"
    score_gap = float(current_top.get("score") or 0) - float(previous_cluster.get("score") or 0)
    if current_top.get("cluster_id") != previous_id and score_gap < NEWS_DIGEST_MAIN_SWITCH_MARGIN:
        candidate_clusters.remove(previous_cluster)
        candidate_clusters.insert(0, previous_cluster)
        return True, f"sujet principal precedent conserve; ecart {score_gap:.1f} < marge {NEWS_DIGEST_MAIN_SWITCH_MARGIN:.1f}"
    return False, "nouveau sujet nettement dominant ou sujet precedent deja premier"


def build_stable_news_digest(
    enriched_news: list[dict],
    categories: list[str],
    force_rebuild: bool = False,
    max_age_minutes: int = NEWS_DIGEST_REUSE_MINUTES,
) -> dict:
    from cache import write_cache

    if not force_rebuild:
        recent = load_recent_news_digest(categories, max_age_minutes=max_age_minutes)
        if recent:
            return recent

    editorial_state = load_editorial_state()
    clusters = build_scored_news_topic_clusters(enriched_news, editorial_state=editorial_state, limit=12)
    kept_previous, main_reason = should_keep_previous_main(clusters, editorial_state)
    selected_clusters = clusters[:6]
    digest_id = hashlib.sha1(
        ("|".join(cluster.get("cluster_id", "") for cluster in selected_clusters) + datetime.now().strftime("%Y-%m-%d-%H")).encode("utf-8")
    ).hexdigest()[:12]

    digest = {
        "digest_id": digest_id,
        "generated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "generated_at_iso": datetime.now(timezone.utc).isoformat(),
        "categories": categories,
        "main_topic": selected_clusters[0] if selected_clusters else {},
        "secondary_topics": selected_clusters[1:6],
        "market_topic": {},
        "selected_cluster_ids": [cluster.get("cluster_id") for cluster in selected_clusters],
        "selection_policy": {
            "reuse_minutes": max_age_minutes,
            "main_switch_margin": NEWS_DIGEST_MAIN_SWITCH_MARGIN,
            "kept_previous_main": kept_previous,
            "main_reason": main_reason,
        },
        "raw_article_count": len(enriched_news),
        "candidate_cluster_count": len(clusters),
    }
    write_cache(NEWS_DIGEST_CACHE_KEY, digest)
    write_editorial_state(digest)
    write_cache(
        NEWS_DIGEST_DEBUG_CACHE_KEY,
        {
            "digest_id": digest_id,
            "generated_at": digest["generated_at_iso"],
            "categories": categories,
            "selection_policy": digest["selection_policy"],
            "selected": selected_clusters,
            "rejected": clusters[6:18],
        },
    )
    return {**digest, "reused": False}


def detect_major_digest_override(digest: dict, fresh_items: list[dict]) -> tuple[bool, str]:
    if not digest or not fresh_items:
        return False, "pas de nouveau flux a comparer"
    current_main = digest.get("main_topic") or {}
    current_score = float(current_main.get("score") or 0)
    clusters = build_scored_news_topic_clusters(fresh_items, editorial_state=load_editorial_state(), limit=3)
    if not clusters:
        return False, "aucun nouveau cluster dominant"
    challenger = clusters[0]
    if challenger.get("cluster_id") == current_main.get("cluster_id"):
        return False, "le sujet principal reste dominant"
    score_gap = float(challenger.get("score") or 0) - current_score
    source_ok = int(challenger.get("source_count") or 0) >= max(2, int(current_main.get("source_count") or 0))
    if score_gap >= 28.0 and source_ok:
        return True, f"nouveau sujet majeur: +{score_gap:.1f} points et {challenger.get('source_count')} sources"
    return False, f"pas de remplacement majeur; ecart {score_gap:.1f}"


def market_snapshot_records() -> list[dict]:
    tickers = tuple(row["ticker"] for row in MARKET_BRIEFING_ASSETS)
    try:
        price_history, return_history = download_price_histories(tickers=tickers, period="1mo", interval="1d")
        snapshot = build_market_snapshot_table(MARKET_BRIEFING_ASSETS, price_history, return_history)
    except Exception:
        return []
    if snapshot.empty:
        return []
    return snapshot.fillna("-").to_dict("records")


def portfolio_briefing_summary(current_user: sqlite3.Row | None) -> dict:
    if current_user is None:
        return {}
    positions = list_portfolio_positions(current_user["username"])
    if not positions:
        return {}
    try:
        portfolio = build_portfolio_frame(positions, load_symbol_catalog(time.time()))
    except Exception:
        return {}
    if portfolio.empty:
        return {}

    priced = portfolio.dropna(subset=["Valeur actuelle"])
    invested = float(portfolio["PRU total"].sum())
    value = float(priced["Valeur actuelle"].sum()) if not priced.empty else 0.0
    pnl = float(priced["PnL latent"].sum()) if not priced.empty else 0.0
    performance = (pnl / float(priced["PRU total"].sum()) * 100) if not priced.empty and float(priced["PRU total"].sum()) else 0.0
    sorted_lines = portfolio.sort_values("PnL latent (%)", ascending=False, na_position="last")
    best = sorted_lines.head(1).fillna("-").to_dict("records")
    worst = sorted_lines.tail(1).fillna("-").to_dict("records")
    tickers = tuple(portfolio["Ticker"].dropna().astype(str).head(8).tolist())
    try:
        ticker_news = [compact_news_item(item) for item in fetch_news_for_tickers(tickers, per_ticker_limit=3)]
    except Exception:
        ticker_news = []
    return {
        "invested": invested,
        "value": value,
        "pnl": pnl,
        "performance_percent": round(performance, 2),
        "line_count": len(portfolio),
        "best_line": best[0] if best else {},
        "worst_line": worst[0] if worst else {},
        "tickers": list(tickers),
        "ticker_news": ticker_news[:12],
    }


def collect_podcast_briefing_context(
    current_user: sqlite3.Row | None,
    categories: list[str],
    include_portfolio: bool = True,
    include_market_context: bool = False,
    items_per_category: int = 8,
    force_digest_rebuild: bool = False,
) -> dict:
    selected_categories = [category for category in categories if category in GENERAL_NEWS_FEEDS] or ["A la une"]
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    category_sections = []
    all_news: list[dict] = []
    digest = load_recent_news_digest(selected_categories) if not force_digest_rebuild else None
    if digest:
        if float(digest.get("cache_age_minutes") or 0) >= 15:
            for category in selected_categories:
                items = sort_general_news_items(fetch_general_news(category), "Plus recentes")[:items_per_category]
                compact_items = [{**compact_news_item(item), "category": category} for item in items]
                category_sections.append({"category": category, "items": compact_items})
                all_news.extend(compact_items)
            major_override, override_reason = detect_major_digest_override(digest, dedupe_compact_news(all_news, limit=45))
            if major_override:
                digest = None
            else:
                digest["major_override_check"] = override_reason

    if digest:
        enriched_news = flatten_digest_reference_articles(digest, limit=12)
        if not category_sections:
            category_sections = [{"category": category, "items": []} for category in selected_categories]
    else:
        if not all_news:
            for category in selected_categories:
                items = sort_general_news_items(fetch_general_news(category), "Plus recentes")[:items_per_category]
                compact_items = [{**compact_news_item(item), "category": category} for item in items]
                category_sections.append({"category": category, "items": compact_items})
                all_news.extend(compact_items)
        for category in selected_categories:
            if not any(section.get("category") == category for section in category_sections):
                category_sections.append({"category": category, "items": []})
        deduped = dedupe_compact_news(all_news, limit=45)
        enriched_news = enrich_news_items_with_content(deduped, top_n=16)
        digest = build_stable_news_digest(
            enriched_news,
            selected_categories,
            force_rebuild=force_digest_rebuild,
            max_age_minutes=NEWS_DIGEST_REUSE_MINUTES,
        )

    if include_market_context:
        try:
            gainers, losers = fetch_market_movers(limit=5)
        except Exception:
            gainers, losers = pd.DataFrame(), pd.DataFrame()

        try:
            midcaps = fetch_midcap_recommendations(limit=5)
        except Exception:
            midcaps = pd.DataFrame()
    else:
        gainers, losers, midcaps = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    digest_clusters = [digest.get("main_topic")] + list(digest.get("secondary_topics") or [])
    digest_clusters = [cluster for cluster in digest_clusters if cluster]

    return {
        "generated_at": generated_at,
        "categories": selected_categories,
        "top_news": flatten_digest_reference_articles(digest, limit=12) or enriched_news,
        "topic_clusters": digest_clusters,
        "news_digest": digest,
        "category_sections": category_sections,
        "market_snapshot": market_snapshot_records() if include_market_context else [],
        "gainers": dataframe_records(gainers, limit=5),
        "losers": dataframe_records(losers, limit=5),
        "midcaps": dataframe_records(midcaps, limit=5),
        "portfolio": portfolio_briefing_summary(current_user) if include_portfolio else {},
        "source_labels": sorted({item["source"] for item in enriched_news if item.get("source")}),
    }


def build_fallback_podcast_script(context: dict, duration_minutes: int) -> str:
    top_news = context.get("top_news", [])[:8]
    topic_clusters = context.get("topic_clusters", [])[:4]
    market_snapshot = context.get("market_snapshot", [])[:10]
    portfolio = context.get("portfolio") or {}
    lines = [
        "Bonjour Rafik.",
        "",
    ]
    if topic_clusters:
        main_topic = topic_clusters[0]
        sources = ", ".join(main_topic.get("sources", [])[:3])
        lines.extend(
            [
                f"On commence par le sujet qui ressort le plus ce matin : {main_topic.get('main_title')}.",
                f"Ce sujet apparait dans {main_topic.get('article_count')} article(s), notamment chez {sources}.",
            ]
        )
        related_titles = [title for title in main_topic.get("related_titles", [])[1:4] if title]
        for title in related_titles:
            lines.append(f"Un angle associe a surveiller : {title}.")
        lines.extend(["", "Ensuite, les autres informations importantes."])
    else:
        lines.append("On va droit au sujet avec les informations importantes du jour.")

    if top_news:
        start_index = 2 if topic_clusters else 1
        for index, item in enumerate(top_news[:8], start=start_index):
            summary = item.get("summary") or "Resume indisponible."
            lines.append(f"{index}. {item.get('title')}. Source : {item.get('source')}. {summary}")
    else:
        lines.append("Aucune information generale n'a pu etre chargee au moment de la generation.")

    if market_snapshot:
        lines.extend(["", "Un point marche rapide, car ce contexte a ete demande."])
        for row in market_snapshot:
            lines.append(
                f"{row.get('Actif')} termine autour de {row.get('Dernier')}, avec une variation 1 jour de {row.get('1j')} pour cent et une variation 1 mois de {row.get('1m')} pour cent."
            )

    if portfolio:
        lines.extend(
            [
                "",
                "Pour le portefeuille simule.",
                f"La valeur suivie est d'environ {portfolio.get('value'):.2f}, pour un PnL latent de {portfolio.get('pnl'):.2f}, soit {portfolio.get('performance_percent')} pour cent.",
            ]
        )
        best = portfolio.get("best_line") or {}
        worst = portfolio.get("worst_line") or {}
        if best:
            lines.append(f"La meilleure ligne est {best.get('Ticker')}, avec une performance de {best.get('PnL latent (%)')} pour cent.")
        if worst:
            lines.append(f"La ligne a surveiller est {worst.get('Ticker')}, avec une performance de {worst.get('PnL latent (%)')} pour cent.")

    lines.extend(
        [
            "",
            "Voila pour l'essentiel. A surveiller surtout : le sujet principal du jour, puis les informations qui reviennent dans plusieurs sources.",
        ]
    )
    return "\n".join(lines)


def build_podcast_script_prompt(context: dict, duration_minutes: int, tone: str) -> str:
    target_words = {3: "550 a 700", 5: "950 a 1200", 10: "2000 a 2500"}.get(duration_minutes, "2000 a 2500")
    word_budget = {
        3: {"principal": "250-300", "secondaires": "100-120 chacun (2 sujets)", "conclusion": "50"},
        5: {"principal": "400-500", "secondaires": "120-150 chacun (3-4 sujets)", "conclusion": "60"},
        10: {"principal": "700-900", "secondaires": "180-250 chacun (5-6 sujets)", "conclusion": "80"},
    }.get(duration_minutes, {"principal": "700-900", "secondaires": "180-250 chacun (5-6 sujets)", "conclusion": "80"})

    parts: list[str] = []
    digest = context.get("news_digest") or {}
    digest_clusters = [digest.get("main_topic")] + list(digest.get("secondary_topics") or [])
    digest_clusters = [cluster for cluster in digest_clusters if cluster]

    if digest_clusters:
        parts.append("=== DIGEST EDITORIAL FIGE ===")
        parts.append(
            f"Digest {digest.get('digest_id', '-')} genere le {digest.get('generated_at', context.get('generated_at', '-'))}. "
            f"Reutilise: {'oui' if digest.get('reused') else 'non'}."
        )
        policy = digest.get("selection_policy") or {}
        if policy.get("main_reason"):
            parts.append(f"Decision sujet principal: {policy.get('main_reason')}")
        for index, cluster in enumerate(digest_clusters, start=1):
            role = "SUJET PRINCIPAL" if index == 1 else f"SUJET SECONDAIRE {index - 1}"
            sources_str = ", ".join(cluster.get("sources", [])[:4])
            reasons = ", ".join(cluster.get("why_selected", [])[:4])
            parts.append(
                f"\n{role}: {cluster.get('main_title')}\n"
                f"Score: {cluster.get('score')} | Articles: {cluster.get('article_count')} | Sources: {sources_str}\n"
                f"Pourquoi retenu: {reasons or '-'}\n"
                f"Resume consolide: {cluster.get('summary') or '-'}"
            )
            for article in cluster.get("reference_articles", [])[:2]:
                snippet = (article.get("content") or article.get("summary") or "")[:900]
                parts.append(f"- Reference [{article.get('source')}]: {article.get('title')} — {snippet}")

    top_news = context.get("top_news", [])
    enriched = [item for item in top_news if item.get("content")]
    flash = [item for item in top_news if not item.get("content")]

    if enriched:
        parts.append("=== ARTICLES PRINCIPAUX (contenu complet disponible) ===")
        for item in enriched[:10]:
            pub = (item.get("published_at") or "")[:10]
            parts.append(f"\n[{item.get('source')} — {pub or 'date inconnue'}]")
            parts.append(f"Titre : {item.get('title')}")
            parts.append(f"Contenu :\n{item.get('content', '')[:1600]}")

    if flash:
        parts.append("\n=== AUTRES TITRES ===")
        for item in flash[:20]:
            summary = (item.get("summary") or "")[:120]
            line = f"• {item.get('title')} [{item.get('source')}]"
            if summary:
                line += f" — {summary}"
            parts.append(line)

    clusters = context.get("topic_clusters", [])
    if clusters:
        parts.append("\n=== SUJETS QUI REVIENNENT DANS PLUSIEURS SOURCES ===")
        for cluster in clusters[:5]:
            sources_str = ", ".join(cluster.get("sources", [])[:3])
            parts.append(
                f"• {cluster.get('main_title')} "
                f"({cluster.get('article_count')} articles — {sources_str})"
            )

    market = context.get("market_snapshot", [])
    if market:
        parts.append("\n=== MARCHES FINANCIERS ===")
        for row in market[:8]:
            parts.append(f"• {row.get('Actif')}: {row.get('Dernier')} — 1j: {row.get('1j')}% — 1m: {row.get('1m')}%")

    portfolio = context.get("portfolio") or {}
    if portfolio and portfolio.get("value"):
        parts.append("\n=== PORTEFEUILLE ===")
        parts.append(
            f"Valeur: {portfolio.get('value', 0):.2f} — "
            f"PnL: {portfolio.get('pnl', 0):.2f} ({portfolio.get('performance_percent')}%)"
        )
        best = portfolio.get("best_line") or {}
        worst = portfolio.get("worst_line") or {}
        if best.get("Ticker"):
            parts.append(f"Meilleure position: {best.get('Ticker')} +{best.get('PnL latent (%)', 0):.1f}%")
        if worst.get("Ticker"):
            parts.append(f"Moins bonne position: {worst.get('Ticker')} {worst.get('PnL latent (%)', 0):.1f}%")
        ticker_news = portfolio.get("ticker_news", [])
        if ticker_news:
            parts.append("Actualites sur les positions :")
            for tn in ticker_news[:5]:
                parts.append(f"  • {tn.get('title')} [{tn.get('source')}]")

    context_text = "\n".join(parts)

    return f"""Tu es redacteur en chef et auteur voix d'un podcast quotidien en francais.

CONTRAINTE DE LONGUEUR ABSOLUE : le script DOIT contenir entre {target_words} mots.
Ne termine PAS avant d'avoir atteint le minimum. Compte mentalement tes mots au fil de la redaction.

Ton : {tone}.

FORMAT AUDIO-FIRST :
- Ecris pour une voix de synthese, pas pour une lecture a l'ecran.
- Le texte doit etre beau a ecouter, fluide, naturel, avec un fil narratif.
- Utilise des phrases respirables, pas trop longues, avec des transitions douces.
- Evite les listes seches, les titres abrupts, les puces, les numerotations et les signes typographiques inutiles.
- N'ecris pas de symboles qui se lisent mal a l'audio : pas de slash, pas de pipe, pas de parenthese technique, pas de markdown, pas d'emoji.
- Remplace les pourcentages par une formulation orale, par exemple "en hausse de trois pour cent".
- Remplace les sigles ou abreges obscurs par une lecture naturelle quand c'est possible.
- Ne dis jamais "ouvrez les guillemets", "deux points", "tiret", "slash", "hashtag" ou une notation technique.

HIERARCHIE EDITORIALE :
- Respecte le DIGEST EDITORIAL FIGE quand il est fourni.
- Le sujet principal du digest DOIT rester le sujet principal du podcast.
- Les sujets secondaires doivent suivre globalement l'ordre du digest.
- Ne remplace pas un sujet du digest par un article isole plus recent.
- Regroupe les articles d'un meme cluster en un seul sujet coherent.

STRUCTURE ET BUDGET DE MOTS :
1. Sujet principal ({word_budget['principal']} mots) :
   - Utilise le sujet principal du digest.
   - Donne le contexte complet : pourquoi c'est important, historique recent si utile, chiffres cles.
   - Compare les angles des differentes sources sur ce sujet.
   - Explique les implications concretes et ce qu'il faut surveiller.
   - Commence par une accroche claire, puis deroule progressivement.

2. Sujets secondaires ({word_budget['secondaires']}) :
   - Pour chaque sujet secondaire : donne le contexte, les faits importants, une implication concrete.
   - Ne fais pas de simples titres lus : developpe vraiment chaque point.
   - Introduis chaque transition avec une phrase naturelle, par exemple "Autre point important", "Dans un autre registre", ou "Pendant ce temps".

3. Point marche ou portefeuille (200 mots si disponible dans le contexte).

4. Conclusion ({word_budget['conclusion']} mots) : une phrase de cloture naturelle, sans formule pompeuse.

Regles strictes :
- Commence par "Bonjour Rafik." puis vas immediatement au contenu.
- N'invente aucun fait absent du contexte ci-dessous.
- Reformule et enrichis, ne recopie pas les articles mot pour mot.
- Cite les sources avec naturalite (ex: "selon Le Monde", "d'apres Les Echos").
- Ne change pas la hierarchie du digest sauf contradiction factuelle evidente dans le contexte.
- Evite les doublons : un cluster = un sujet, meme si plusieurs titres sont proches.
- Ecris uniquement le texte final pret a etre entendu, sans JSON, sans commentaires techniques, sans titres de section.
- Ne laisse aucun artefact visuel : pas de crochets, pas de markdown, pas de bullet points, pas de lien brut.
- Si le contexte est riche, developpe davantage. Mieux vaut 2400 mots que 1800 pour un podcast de 10 minutes.

--- CONTEXTE DU JOUR ---
{context_text}
--- FIN DU CONTEXTE ---""".strip()


def extract_openai_output_text(payload: dict) -> str:
    if payload.get("output_text"):
        return str(payload["output_text"]).strip()
    pieces: list[str] = []
    for output_item in payload.get("output", []) or []:
        for content in output_item.get("content", []) or []:
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                pieces.append(str(content["text"]))
    return "\n".join(pieces).strip()


def normalize_script_for_audio(script: str) -> str:
    """Nettoie les artefacts visuels qui passent mal en synthese vocale."""
    text = script or ""
    replacements = {
        "\u2022": "",
        "\u2014": ", ",
        "\u2013": ", ",
        "|": ", ",
        " / ": " ou ",
        "&": " et ",
        "#": "numero ",
        "%": " pour cent",
        "€": " euros",
        "$": " dollars",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)

    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]+)\*", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"(?<!\w)\+(\d)", r"plus \1", text)
    text = re.sub(r"(?m)^\s*[-*]\s+", "", text)
    text = re.sub(r"(?m)^\s*\d+\.\s+", "", text)
    text = re.sub(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF]+", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"([.!?]){2,}", r"\1", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def select_top_articles_with_llm(items: list[dict], api_key: str, n: int = 12) -> list[dict]:
    """Passe 1 : demande au LLM de choisir les articles les plus importants."""
    if not api_key or len(items) <= n:
        return items[:n]

    lines = []
    for i, item in enumerate(items[:35]):
        snippet = (item.get("content") or item.get("summary") or "")[:200]
        lines.append(f"{i + 1}. [{item.get('source')}] {item.get('title')} — {snippet}")

    prompt = (
        f"Tu es un editeur de presse. Voici {len(lines)} articles du jour.\n\n"
        f"Selectionne les {n} articles les plus importants pour un briefing quotidien generaliste en francais.\n"
        "Criteres : importance nationale ou internationale, impact societal ou economique, diversite des sujets.\n"
        f"Reponds UNIQUEMENT avec les numeros separes par des virgules. Exemple : 2, 5, 7, 12\n\n"
        + "\n".join(lines)
    )
    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": PODCAST_SCRIPT_MODEL, "input": prompt, "max_output_tokens": 80},
            timeout=30,
        )
        response.raise_for_status()
        text = extract_openai_output_text(response.json()).strip()
        indices = [int(x.strip()) - 1 for x in re.split(r"[,\s]+", text) if x.strip().isdigit()]
        selected = [items[i] for i in indices if 0 <= i < len(items)]
        if len(selected) >= 5:
            return selected[:n]
    except Exception:
        pass
    return items[:n]


def generate_podcast_script(context: dict, duration_minutes: int, tone: str) -> tuple[str, str]:
    api_key = get_openai_api_key()
    if not api_key:
        return build_fallback_podcast_script(context, duration_minutes), "fallback"

    # La selection editoriale est deja figee dans news_digest. Le LLM redige,
    # mais ne re-hierarchise pas librement les articles.
    prompt = build_podcast_script_prompt(context, duration_minutes, tone)
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": PODCAST_SCRIPT_MODEL,
            "input": prompt,
            "max_output_tokens": 6000,
        },
        timeout=120,
    )
    response.raise_for_status()
    script = extract_openai_output_text(response.json())
    if not script:
        raise ValueError("Le modele n'a pas renvoye de script exploitable.")
    return normalize_script_for_audio(script), "openai"


def split_tts_script(script: str, max_chars: int = 3800) -> list[str]:
    script = normalize_script_for_audio(script)
    paragraphs = [paragraph.strip() for paragraph in re.split(r"\n\s*\n", script.strip()) if paragraph.strip()]
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        if len(paragraph) > max_chars:
            sentences = re.split(r"(?<=[.!?])\s+", paragraph)
            for sentence in sentences:
                if len(current) + len(sentence) + 1 > max_chars and current:
                    chunks.append(current.strip())
                    current = ""
                current += (" " if current else "") + sentence
            continue
        if len(current) + len(paragraph) + 2 > max_chars and current:
            chunks.append(current.strip())
            current = paragraph
        else:
            current += ("\n\n" if current else "") + paragraph
    if current.strip():
        chunks.append(current.strip())
    return chunks or [script.strip()]


def request_tts_audio_chunk(text: str, voice: str, instructions: str) -> bytes:
    api_key = get_openai_api_key()
    response = requests.post(
        "https://api.openai.com/v1/audio/speech",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": PODCAST_TTS_MODEL,
            "voice": voice,
            "input": text,
            "instructions": instructions,
            "response_format": "mp3",
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.content


def generate_podcast_audio_file(script: str, output_path: Path, voice: str, instructions: str) -> Path:
    api_key = get_openai_api_key()
    if not api_key:
        raise ValueError("OPENAI_API_KEY est manquant : impossible de generer l'audio.")
    chunks = split_tts_script(script)
    audio_parts = [request_tts_audio_chunk(chunk, voice, instructions) for chunk in chunks]
    output_path.write_bytes(b"".join(audio_parts))
    return output_path


def generate_local_espeak_audio_file(script: str, output_path: Path, engine_path: str) -> Path:
    if not engine_path:
        raise ValueError("Aucun moteur espeak local n'est disponible.")
    wav_path = output_path.with_suffix(".wav")
    input_text = normalize_script_for_audio(script)
    if not input_text:
        raise ValueError("Le script est vide.")
    subprocess.run(
        [
            engine_path,
            "-v",
            "fr-fr",
            "-s",
            "155",
            "-w",
            str(wav_path),
            input_text,
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=180,
    )
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path and output_path.suffix.lower() == ".mp3":
        subprocess.run(
            [
                ffmpeg_path,
                "-y",
                "-i",
                str(wav_path),
                "-codec:a",
                "libmp3lame",
                "-q:a",
                "4",
                str(output_path),
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=180,
        )
        try:
            wav_path.unlink()
        except OSError:
            pass
        return output_path
    return wav_path


def save_podcast_assets(context: dict, script: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "context.json").write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (output_dir / "script.md").write_text(script, encoding="utf-8")


def build_price_figure(
    history: pd.DataFrame,
    label_by_ticker: dict[str, str],
    compress_time_axis: bool = True,
    primary_ticker: str | None = None,
) -> go.Figure:
    fig = go.Figure()
    x_values = format_chart_index(history.index) if compress_time_axis else history.index
    for ticker in history.columns:
        is_primary = ticker == primary_ticker
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=history[ticker],
                mode="lines",
                connectgaps=True,
                name=label_by_ticker.get(ticker, ticker),
                line=dict(width=3.2 if is_primary else 1.7),
                opacity=1.0 if is_primary else 0.72,
                hovertemplate="Date : %{x}<br>Prix : %{y:.2f}<extra></extra>",
            )
        )

    fig.update_layout(
        title="Comparaison des cours",
        xaxis_title="Date",
        yaxis_title="Prix de cloture",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=30, r=30, t=60, b=30),
        legend_title="Actifs",
    )
    if compress_time_axis:
        fig.update_xaxes(type="category")
    return fig


def build_performance_figure(
    performance: pd.DataFrame,
    label_by_ticker: dict[str, str],
    compress_time_axis: bool = True,
    primary_ticker: str | None = None,
) -> go.Figure:
    fig = go.Figure()
    x_values = format_chart_index(performance.index) if compress_time_axis else performance.index
    for ticker in performance.columns:
        is_primary = ticker == primary_ticker
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=performance[ticker],
                mode="lines",
                connectgaps=True,
                name=label_by_ticker.get(ticker, ticker),
                line=dict(width=3.2 if is_primary else 1.7),
                opacity=1.0 if is_primary else 0.72,
                hovertemplate="Date : %{x}<br>Performance : %{y:.2f}%<extra></extra>",
            )
        )

    fig.update_layout(
        title="Performance cumulee sur la periode",
        xaxis_title="Date",
        yaxis_title="Performance (%)",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=30, r=30, t=60, b=30),
        legend_title="Actifs",
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    if compress_time_axis:
        fig.update_xaxes(type="category")
    return fig


def compute_period_change(series: pd.Series, lookback_points: int) -> float | None:
    cleaned = series.dropna()
    if len(cleaned) <= lookback_points:
        return None
    end_value = float(cleaned.iloc[-1])
    start_value = float(cleaned.iloc[-lookback_points - 1])
    if start_value == 0:
        return None
    return (end_value / start_value - 1) * 100


def infer_market_sentiment(day_change: float | None, month_change: float | None, news_items: list[dict]) -> tuple[str, str]:
    score = 0
    reasons: list[str] = []

    if day_change is not None:
        if day_change >= 2:
            score += 1
            reasons.append("bonne seance")
        elif day_change <= -2:
            score -= 1
            reasons.append("seance faible")

    if month_change is not None:
        if month_change >= 8:
            score += 2
            reasons.append("tendance 1 mois solide")
        elif month_change >= 3:
            score += 1
            reasons.append("tendance 1 mois positive")
        elif month_change <= -8:
            score -= 2
            reasons.append("tendance 1 mois degradee")
        elif month_change <= -3:
            score -= 1
            reasons.append("tendance 1 mois negative")

    positive_words = ("beat", "surge", "growth", "record", "expands", "deal", "rises", "upgrade")
    negative_words = ("miss", "cuts", "probe", "delay", "falls", "drop", "lawsuit", "warning")
    for item in news_items[:5]:
        title = (item.get("title") or "").lower()
        if any(word in title for word in positive_words):
            score += 1
        if any(word in title for word in negative_words):
            score -= 1

    if score >= 3:
        return "Positif", ", ".join(reasons[:3]) or "momentum et news plutot favorables"
    if score <= -3:
        return "Negatif", ", ".join(reasons[:3]) or "momentum et news plutot defavorables"
    return "Mitige", ", ".join(reasons[:3]) or "signaux partages"


def format_money(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    value = float(value)
    if abs(value) >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    return f"${value:,.0f}".replace(",", " ")


def normalize_ticker(value: str) -> str:
    return (value or "").strip().upper()


def parse_float_value(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)
    cleaned = str(value).strip().replace(" ", "").replace(",", ".")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def format_percent(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):+.2f}%"


def format_decimal_percent(value: float | int | None, signed: bool = False) -> str:
    if value is None or pd.isna(value):
        return "-"
    sign = "+" if signed else ""
    return f"{float(value) * 100:{sign}.2f}%"


def format_ratio(value: float | int | None, suffix: str = "x") -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}{suffix}"


def format_optional_date(timestamp_value: object) -> str:
    if timestamp_value in (None, "", "-"):
        return "-"
    if isinstance(timestamp_value, (list, tuple)) and timestamp_value:
        timestamp_value = timestamp_value[0]
    try:
        if pd.isna(timestamp_value):
            return "-"
    except (TypeError, ValueError):
        return "-"
    try:
        if isinstance(timestamp_value, (int, float)):
            return datetime.fromtimestamp(float(timestamp_value), tz=timezone.utc).strftime("%d/%m/%Y")
        parsed = pd.to_datetime(timestamp_value, errors="coerce")
        if pd.isna(parsed):
            return "-"
        return parsed.strftime("%d/%m/%Y")
    except Exception:
        return str(timestamp_value)


def get_position_catalog_metadata(ticker: str, catalog: pd.DataFrame) -> dict:
    ticker = normalize_ticker(ticker)
    matches = catalog[catalog["ticker"] == ticker]
    if matches.empty:
        return {"name": ticker, "asset_type": "Actif", "exchange": "-"}
    row = matches.iloc[0]
    return {
        "name": row.get("name") or ticker,
        "asset_type": row.get("asset_type") or "Actif",
        "exchange": row.get("exchange") or "-",
    }


def list_portfolio_positions(username: str) -> list[dict]:
    normalized = normalize_username(username)
    with get_user_db_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, ticker, quantity, purchase_price, purchase_date, currency, note, created_at, updated_at
            FROM portfolio_positions
            WHERE username = ? COLLATE NOCASE
            ORDER BY purchase_date ASC, ticker ASC, id ASC
            """,
            (normalized,),
        ).fetchall()
    return [dict(row) for row in rows]


def add_portfolio_position(
    username: str,
    ticker: str,
    quantity: float,
    purchase_price: float,
    purchase_date: str,
    currency: str = "",
    note: str = "",
) -> None:
    normalized_username = normalize_username(username)
    normalized_ticker = normalize_ticker(ticker)
    if not normalized_ticker:
        raise ValueError("Ticker manquant.")
    if quantity <= 0:
        raise ValueError("La quantite doit etre positive.")
    if purchase_price <= 0:
        raise ValueError("Le prix d'achat doit etre positif.")
    try:
        parsed_date = pd.to_datetime(purchase_date, errors="raise").date().isoformat()
    except Exception as exc:
        raise ValueError("Date d'achat invalide.") from exc

    now = utc_iso()
    with get_user_db_connection() as connection:
        connection.execute(
            """
            INSERT INTO portfolio_positions (
                username, ticker, quantity, purchase_price, purchase_date, currency, note, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_username,
                normalized_ticker,
                float(quantity),
                float(purchase_price),
                parsed_date,
                (currency or "").strip().upper(),
                (note or "").strip(),
                now,
                now,
            ),
        )


def delete_portfolio_position(username: str, position_id: int) -> None:
    normalized_username = normalize_username(username)
    with get_user_db_connection() as connection:
        connection.execute(
            """
            DELETE FROM portfolio_positions
            WHERE id = ? AND username = ? COLLATE NOCASE
            """,
            (int(position_id), normalized_username),
        )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_latest_prices(tickers: tuple[str, ...]) -> dict[str, float]:
    if yf is None or not tickers:
        return {}

    prices: dict[str, float] = {}

    def collect_from_history(period: str, interval: str, pending: tuple[str, ...]) -> None:
        if not pending:
            return
        try:
            price_history, _ = download_price_histories(pending, period=period, interval=interval)
        except Exception:
            return
        for ticker in pending:
            if ticker not in price_history.columns:
                continue
            series = price_history[ticker].dropna()
            if not series.empty:
                prices[ticker] = float(series.iloc[-1])

    # Fresh intraday prices first, then robust daily fallback for tickers that
    # Yahoo does not return reliably in 5-minute mode.
    collect_from_history("2d", "5m", tickers)
    missing = tuple(ticker for ticker in tickers if ticker not in prices)
    collect_from_history("10d", "1d", missing)

    for ticker in tickers:
        if ticker in prices:
            continue
        try:
            fast_info = yf.Ticker(ticker).fast_info
            fallback_price = (
                fast_info.get("lastPrice")
                or fast_info.get("last_price")
                or fast_info.get("regularMarketPrice")
                or fast_info.get("previousClose")
            )
            if fallback_price is not None:
                prices[ticker] = float(fallback_price)
        except Exception:
            continue
    return prices


@st.cache_data(ttl=900, show_spinner=False)
def fetch_reference_purchase_price(ticker: str, purchase_date: str) -> float | None:
    if yf is None:
        return None
    ticker = normalize_ticker(ticker)
    parsed_date = pd.to_datetime(purchase_date, errors="coerce")
    if not ticker or pd.isna(parsed_date):
        return None

    today = pd.Timestamp(datetime.now().date())
    if parsed_date.normalize() >= today - pd.Timedelta(days=5):
        try:
            return fetch_latest_prices((ticker,)).get(ticker)
        except Exception:
            return None

    start = parsed_date.normalize()
    end = start + pd.Timedelta(days=10)
    try:
        raw = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval="1d",
        auto_adjust=True,
            progress=False,
            threads=False,
            group_by="ticker",
            multi_level_index=True,
        )
    except Exception:
        return None
    if raw.empty:
        return None

    try:
        history = extract_history_series(raw, (ticker,), preferred_field="Close")
    except Exception:
        return None
    series = history[ticker].dropna() if ticker in history.columns else pd.Series(dtype=float)
    if series.empty:
        return None
    return float(series.iloc[0])


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ticker_metadata(ticker: str) -> dict:
    if yf is None:
        return {}
    try:
        info = yf.Ticker(ticker).get_info()
    except Exception:
        return {}
    return {
        "currency": info.get("currency") or info.get("financialCurrency") or "",
        "sector": info.get("sector") or "-",
        "industry": info.get("industry") or "-",
        "market_cap": info.get("marketCap"),
        "name": info.get("longName") or info.get("shortName") or ticker,
    }


def build_portfolio_frame(positions: list[dict], catalog: pd.DataFrame) -> pd.DataFrame:
    if not positions:
        return pd.DataFrame()

    tickers = tuple(sorted({normalize_ticker(position["ticker"]) for position in positions}))
    try:
        latest_prices = fetch_latest_prices(tickers)
    except Exception:
        latest_prices = {}

    metadata_by_ticker = {ticker: fetch_ticker_metadata(ticker) for ticker in tickers}
    rows = []
    for position in positions:
        ticker = normalize_ticker(position["ticker"])
        catalog_meta = get_position_catalog_metadata(ticker, catalog)
        provider_meta = metadata_by_ticker.get(ticker, {})
        quantity = float(position["quantity"])
        purchase_price = float(position["purchase_price"])
        cost_basis = quantity * purchase_price
        last_price = latest_prices.get(ticker)
        current_value = quantity * last_price if last_price is not None else None
        pnl = current_value - cost_basis if current_value is not None else None
        pnl_percent = (pnl / cost_basis) * 100 if pnl is not None and cost_basis else None

        rows.append(
            {
                "ID": int(position["id"]),
                "Nom": provider_meta.get("name") or catalog_meta["name"],
                "Ticker": ticker,
                "Type": catalog_meta["asset_type"],
                "Secteur": provider_meta.get("sector") or "-",
                "Devise": position.get("currency") or provider_meta.get("currency") or "-",
                "Quantite": quantity,
                "Prix achat": purchase_price,
                "Date achat": position["purchase_date"],
                "Dernier cours": last_price,
                "PRU total": cost_basis,
                "Valeur actuelle": current_value,
                "PnL latent": pnl,
                "PnL latent (%)": pnl_percent,
                "Note": position.get("note") or "",
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    total_pnl = frame["PnL latent"].dropna().sum()
    if total_pnl:
        frame["Contribution perf (%)"] = frame["PnL latent"].fillna(0) / abs(total_pnl) * 100
    else:
        frame["Contribution perf (%)"] = 0.0
    return frame.sort_values("Valeur actuelle", ascending=False, na_position="last").reset_index(drop=True)


def choose_history_period_from_date(start_date: str) -> str:
    parsed = pd.to_datetime(start_date, errors="coerce")
    if pd.isna(parsed):
        return "1y"
    age_days = max(1, (pd.Timestamp(datetime.now().date()) - parsed.normalize()).days)
    if age_days <= 31:
        return "1mo"
    if age_days <= 93:
        return "3mo"
    if age_days <= 186:
        return "6mo"
    if age_days <= 370:
        return "1y"
    if age_days <= 370 * 2:
        return "2y"
    if age_days <= 370 * 5:
        return "5y"
    return "max"


def build_portfolio_performance_history(portfolio: pd.DataFrame) -> pd.DataFrame:
    if portfolio.empty:
        return pd.DataFrame()
    priced = portfolio.dropna(subset=["Quantite", "Prix achat", "Date achat"]).copy()
    if priced.empty:
        return pd.DataFrame()

    earliest_date = str(priced["Date achat"].min())
    start_date = pd.to_datetime(earliest_date, errors="coerce")
    if pd.isna(start_date):
        return pd.DataFrame()
    age_days = max(1, (pd.Timestamp(datetime.now().date()) - start_date.normalize()).days)
    if age_days <= 7:
        period = "5d"
        interval = "30m"
    else:
        period = choose_history_period_from_date(earliest_date)
        interval = "1d"
    tickers = tuple(sorted(priced["Ticker"].unique()))
    try:
        price_history, _ = download_price_histories(tickers, period=period, interval=interval)
    except Exception:
        return pd.DataFrame()

    if isinstance(price_history.index, pd.DatetimeIndex):
        price_history = price_history[price_history.index.date >= start_date.date()]
    if price_history.empty:
        return pd.DataFrame()

    value = pd.Series(0.0, index=price_history.index)
    invested = pd.Series(0.0, index=price_history.index)
    for _, row in priced.iterrows():
        ticker = row["Ticker"]
        if ticker not in price_history.columns:
            continue
        purchase_date = pd.to_datetime(row["Date achat"], errors="coerce")
        if pd.isna(purchase_date):
            continue
        active_mask = price_history.index.date >= purchase_date.date()
        line_prices = price_history[ticker].ffill()
        quantity = float(row["Quantite"])
        cost_basis = float(row["Quantite"]) * float(row["Prix achat"])
        value = value.add((line_prices * quantity).where(active_mask, 0.0), fill_value=0.0)
        invested = invested.add(pd.Series(cost_basis, index=price_history.index).where(active_mask, 0.0), fill_value=0.0)

    history = pd.DataFrame({"Valeur": value, "Investi": invested})
    history = history[history["Investi"] > 0].copy()
    if history.empty:
        return history
    history["Performance (%)"] = (history["Valeur"] / history["Investi"] - 1) * 100
    history["PnL"] = history["Valeur"] - history["Investi"]
    return history


def build_portfolio_performance_figure(history: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if history.empty:
        return fig
    x_values = format_chart_index(history.index)
    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=history["Valeur"],
            mode="lines",
            name="Valeur du portefeuille",
            line=dict(color="#2563eb", width=3),
            hovertemplate="Date : %{x}<br>Valeur : %{y:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Valeur du portefeuille",
        xaxis_title="Date",
        yaxis_title="Montant",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=30, r=30, t=60, b=30),
        showlegend=False,
    )
    fig.update_xaxes(type="category")
    return fig


def build_allocation_frame(portfolio: pd.DataFrame, group_column: str) -> pd.DataFrame:
    if portfolio.empty or group_column not in portfolio:
        return pd.DataFrame()
    values = portfolio.dropna(subset=["Valeur actuelle"]).copy()
    if values.empty:
        return pd.DataFrame()
    grouped = values.groupby(group_column, dropna=False)["Valeur actuelle"].sum().reset_index()
    total = grouped["Valeur actuelle"].sum()
    grouped["Poids (%)"] = grouped["Valeur actuelle"] / total * 100 if total else 0.0
    return grouped.sort_values("Valeur actuelle", ascending=False).reset_index(drop=True)


def build_allocation_figure(allocation: pd.DataFrame, label_column: str) -> go.Figure:
    fig = go.Figure()
    if allocation.empty:
        return fig
    fig.add_trace(
        go.Pie(
            labels=allocation[label_column],
            values=allocation["Valeur actuelle"],
            hole=0.46,
            hovertemplate="%{label}<br>Valeur : %{value:.2f}<br>Poids : %{percent}<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=True,
    )
    return fig


@st.cache_data(ttl=900, show_spinner=False)
def fetch_benchmark_returns(start_date: str) -> pd.DataFrame:
    if yf is None:
        return pd.DataFrame()
    parsed_start = pd.to_datetime(start_date, errors="coerce")
    if pd.isna(parsed_start):
        return pd.DataFrame()
    start = max(parsed_start.date(), (datetime.now().date() - timedelta(days=365 * 5)))
    tickers = tuple(BENCHMARK_TICKERS.values())
    try:
        _, return_history = download_price_histories(tickers, period="5y", interval="1d")
    except Exception:
        return pd.DataFrame()

    return_history = return_history[return_history.index.date >= start]
    if return_history.empty:
        return pd.DataFrame()
    performance = compute_performance_frame(return_history)
    rows = []
    for label, ticker in BENCHMARK_TICKERS.items():
        if ticker not in performance.columns:
            continue
        series = performance[ticker].dropna()
        if series.empty:
            continue
        rows.append({"Benchmark": label, "Ticker": ticker, "Performance depuis debut (%)": round(float(series.iloc[-1]), 2)})
    return pd.DataFrame(rows)


def build_market_snapshot_table(asset_rows: list[dict], history: pd.DataFrame, return_history: pd.DataFrame) -> pd.DataFrame:
    rows = []
    by_ticker = {row["ticker"]: row for row in asset_rows}
    for ticker in history.columns:
        price_series = history[ticker].dropna()
        return_series = return_history[ticker].dropna() if ticker in return_history.columns else price_series
        if price_series.empty or return_series.empty:
            continue
        rows.append(
            {
                "Groupe": by_ticker.get(ticker, {}).get("group", "-"),
                "Actif": by_ticker.get(ticker, {}).get("name", ticker),
                "Ticker": ticker,
                "Dernier": round(float(price_series.iloc[-1]), 2),
                "1j": compute_period_change(return_series, 1),
                "1m": compute_period_change(return_series, 21),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["1j"] = frame["1j"].round(2)
    frame["1m"] = frame["1m"].round(2)
    return frame.sort_values(["Groupe", "Actif"]).reset_index(drop=True)


def classify_valuation(pe: float | None, ps: float | None, ev_to_ebitda: float | None) -> tuple[str, str]:
    available = [value for value in (pe, ps, ev_to_ebitda) if value is not None and not pd.isna(value) and value > 0]
    if not available:
        return "Indisponible", "Ratios insuffisants pour juger la valorisation."
    expensive_flags = int(pe is not None and pe > 35) + int(ps is not None and ps > 8) + int(ev_to_ebitda is not None and ev_to_ebitda > 20)
    cheap_flags = int(pe is not None and 0 < pe < 16) + int(ps is not None and 0 < ps < 3) + int(ev_to_ebitda is not None and 0 < ev_to_ebitda < 10)
    if expensive_flags >= 2:
        return "Chere", "Plusieurs multiples sont tendus."
    if cheap_flags >= 2:
        return "Peu chere", "Plusieurs multiples restent contenus."
    return "Neutre", "La valorisation ne ressort pas extreme avec les ratios disponibles."


def classify_growth(revenue_growth: float | None, earnings_growth: float | None) -> tuple[str, str]:
    values = [value for value in (revenue_growth, earnings_growth) if value is not None and not pd.isna(value)]
    if not values:
        return "Indisponible", "Croissance non disponible."
    best = max(values)
    if best >= 0.15:
        return "Forte", "La croissance publiee/attendue ressort superieure a 15%."
    if best >= 0.05:
        return "Moderee", "La croissance ressort positive mais pas explosive."
    return "Faible", "La croissance disponible est faible ou negative."


def classify_risk(
    beta: float | None,
    debt_to_equity: float | None,
    operating_margin: float | None,
    free_cashflow: float | None,
) -> tuple[str, str]:
    score = 0
    reasons = []
    if beta is not None and not pd.isna(beta) and beta > 1.5:
        score += 1
        reasons.append("beta eleve")
    if debt_to_equity is not None and not pd.isna(debt_to_equity) and debt_to_equity > 150:
        score += 1
        reasons.append("endettement important")
    if operating_margin is not None and not pd.isna(operating_margin) and operating_margin < 0:
        score += 1
        reasons.append("marge operationnelle negative")
    if free_cashflow is not None and not pd.isna(free_cashflow) and free_cashflow < 0:
        score += 1
        reasons.append("free cash flow negatif")
    if score >= 2:
        return "Eleve", ", ".join(reasons[:3])
    if score == 1:
        return "Moyen", reasons[0]
    return "Faible", "Pas de signal de risque majeur dans les donnees disponibles."


COMMON_PEERS = {
    "AAPL": ["MSFT", "GOOGL", "AMZN"],
    "MSFT": ["AAPL", "GOOGL", "ORCL"],
    "GOOGL": ["META", "MSFT", "AMZN"],
    "META": ["GOOGL", "SNAP", "PINS"],
    "NVDA": ["AMD", "AVGO", "INTC"],
    "AMD": ["NVDA", "INTC", "AVGO"],
    "TSLA": ["GM", "F", "RIVN"],
    "JPM": ["BAC", "WFC", "C"],
    "BAC": ["JPM", "WFC", "C"],
    "XOM": ["CVX", "COP", "SHEL"],
    "NKE": ["LULU", "ADDYY", "UAA"],
}


def suggest_peer_tickers(ticker: str, sector: str) -> list[str]:
    ticker = normalize_ticker(ticker)
    if ticker in COMMON_PEERS:
        return COMMON_PEERS[ticker]
    sector_lower = (sector or "").lower()
    if "technology" in sector_lower:
        return ["MSFT", "AAPL", "GOOGL"]
    if "financial" in sector_lower:
        return ["JPM", "BAC", "WFC"]
    if "energy" in sector_lower:
        return ["XOM", "CVX", "COP"]
    if "healthcare" in sector_lower:
        return ["JNJ", "PFE", "MRK"]
    return []


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_peer_comparison(tickers: tuple[str, ...]) -> pd.DataFrame:
    rows = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).get_info() if yf is not None else {}
        except Exception:
            info = {}
        if not info:
            continue
        rows.append(
            {
                "Nom": info.get("shortName") or info.get("longName") or ticker,
                "Ticker": ticker,
                "Capitalisation": info.get("marketCap"),
                "CA": info.get("totalRevenue"),
                "Marge op.": info.get("operatingMargins"),
                "Croissance CA": info.get("revenueGrowth"),
                "PER": info.get("trailingPE") or info.get("forwardPE"),
                "Dette/Equity": info.get("debtToEquity"),
                "Perf 1 an": info.get("52WeekChange"),
            }
        )
    return pd.DataFrame(rows)


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_company_snapshot(ticker: str) -> dict:
    if yf is None:
        raise RuntimeError("yfinance n'est pas disponible.")

    stock = yf.Ticker(ticker)
    info = stock.get_info()

    revenue = info.get("totalRevenue")
    if revenue is None:
        try:
            income_stmt = stock.income_stmt
            if not income_stmt.empty and "Total Revenue" in income_stmt.index:
                revenue = float(income_stmt.loc["Total Revenue"].dropna().iloc[0])
        except Exception:
            revenue = None

    price_history, return_history = download_price_histories((ticker,), period="3mo", interval="1d")
    price_series = price_history[ticker].dropna()
    return_series = return_history[ticker].dropna()

    day_change = compute_period_change(return_series, 1) if len(return_series) > 1 else None
    month_change = compute_period_change(return_series, 21)
    news_items = fetch_news_for_tickers((ticker,), per_ticker_limit=6)
    sentiment_label, sentiment_reason = infer_market_sentiment(day_change, month_change, news_items)

    trailing_pe = info.get("trailingPE") or info.get("forwardPE")
    price_to_sales = info.get("priceToSalesTrailing12Months")
    ev_to_ebitda = info.get("enterpriseToEbitda")
    gross_margin = info.get("grossMargins")
    operating_margin = info.get("operatingMargins")
    revenue_growth = info.get("revenueGrowth")
    earnings_growth = info.get("earningsGrowth")
    debt_to_equity = info.get("debtToEquity")
    free_cashflow = info.get("freeCashflow")
    beta = info.get("beta")
    valuation_label, valuation_reason = classify_valuation(trailing_pe, price_to_sales, ev_to_ebitda)
    growth_label, growth_reason = classify_growth(revenue_growth, earnings_growth)
    risk_label, risk_reason = classify_risk(beta, debt_to_equity, operating_margin, free_cashflow)

    summary = str(info.get("longBusinessSummary") or "").strip()
    if summary:
        summary = summary.split(". ")[0].strip()
        if not summary.endswith("."):
            summary += "."

    return {
        "name": info.get("longName") or info.get("shortName") or ticker,
        "ticker": ticker,
        "revenue": revenue,
        "sector": info.get("sector") or "-",
        "industry": info.get("industry") or "-",
        "summary": summary or "Resume indisponible.",
        "market_cap": info.get("marketCap"),
        "day_change": day_change,
        "month_change": month_change,
        "last_price": float(price_series.iloc[-1]) if not price_series.empty else None,
        "sentiment_label": sentiment_label,
        "sentiment_reason": sentiment_reason,
        "trailing_pe": trailing_pe,
        "price_to_sales": price_to_sales,
        "ev_to_ebitda": ev_to_ebitda,
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
        "revenue_growth": revenue_growth,
        "earnings_growth": earnings_growth,
        "debt_to_equity": debt_to_equity,
        "total_debt": info.get("totalDebt"),
        "total_cash": info.get("totalCash"),
        "free_cashflow": free_cashflow,
        "dividend_yield": info.get("dividendYield"),
        "next_earnings": info.get("earningsTimestamp") or info.get("earningsTimestampStart") or info.get("earningsDate"),
        "target_price": info.get("targetMeanPrice"),
        "analyst_count": info.get("numberOfAnalystOpinions"),
        "beta": beta,
        "valuation_label": valuation_label,
        "valuation_reason": valuation_reason,
        "growth_label": growth_label,
        "growth_reason": growth_reason,
        "risk_label": risk_label,
        "risk_reason": risk_reason,
        "peer_tickers": suggest_peer_tickers(ticker, info.get("sector") or ""),
        "news_items": news_items[:3],
    }


def build_summary_table(
    catalog: pd.DataFrame,
    price_history: pd.DataFrame,
    return_history: pd.DataFrame,
    regular_price_history: pd.DataFrame | None = None,
    include_prepost: bool = False,
) -> pd.DataFrame:
    rows = []
    catalog_by_ticker = catalog.set_index("ticker")
    for ticker in price_history.columns:
        price_series = price_history[ticker].dropna()
        if price_series.empty:
            continue

        return_series = return_history[ticker].dropna() if ticker in return_history.columns else price_series
        start_price = float(price_series.iloc[0])
        end_price = float(price_series.iloc[-1])
        base_return = float(return_series.iloc[0]) if not return_series.empty else 0.0
        end_return = float(return_series.iloc[-1]) if not return_series.empty else 0.0
        performance = ((end_return / base_return) - 1) * 100 if base_return else 0.0
        row = catalog_by_ticker.loc[ticker]
        asset_type = row["asset_type"]
        session_raw = infer_market_session(ticker, asset_type=asset_type)
        session_labels = {
            "regular": "🟢 Regular",
            "pre_market": "🟡 Pre-market",
            "after_hours": "🔵 After-hours",
            "closed": "⚪ Closed",
            "24/7": "🟢 24/7",
            "unknown": "⚪ Unknown",
        }
        regular_price = None
        if regular_price_history is not None and ticker in regular_price_history.columns:
            regular_series = regular_price_history[ticker].dropna()
            if not regular_series.empty:
                regular_price = float(regular_series.iloc[-1])
        prepost_price = None
        if include_prepost and regular_price is not None and abs(end_price - regular_price) > max(0.0001, abs(regular_price) * 0.000001):
            prepost_price = end_price
        price_source = "Regular"
        principal_price = regular_price if regular_price is not None else end_price
        if session_raw in {"pre_market", "after_hours"} and include_prepost and prepost_price is not None:
            principal_price = prepost_price
            price_source = "Pre-market" if session_raw == "pre_market" else "After-hours"
        elif session_raw == "closed":
            price_source = "Cloture"
        elif regular_price is None:
            price_source = "Dernier point Yahoo"
        last_timestamp = price_series.index[-1]
        if hasattr(last_timestamp, "strftime"):
            last_timestamp = last_timestamp.strftime("%d/%m/%Y %H:%M")
        rows.append(
            {
                "Nom": row["name"],
                "Ticker": ticker,
                "Actif": f"{row['name']} ({ticker})",
                "Type": row["asset_type"],
                "Region": row.get("market_region") or row.get("region") or infer_market_region(ticker, row.get("asset_type")),
                "Marche": row["exchange"] or "-",
                "Devise": row.get("currency") or infer_currency(ticker, default="USD"),
                "Session": session_labels.get(session_raw, "⚪ Unknown"),
                "Session brute": session_raw,
                "Dernier timestamp": last_timestamp,
                "Debut periode": round(start_price, 2),
                "Prix": round(principal_price, 2),
                "Source prix": price_source,
                "Prix regular": round(regular_price, 2) if regular_price is not None else "-",
                "Prix affiche": round(end_price, 2),
                "Prix pre/post": round(prepost_price, 2) if prepost_price is not None else "-",
                "Variation (%)": round(performance, 2),
            }
        )

    return pd.DataFrame(rows).reset_index(drop=True)


def render_header(catalog: pd.DataFrame, cache_path: Path) -> None:
    market_regime = "Neutre"
    market_score = "-"
    market_reason = "Ouvre Marche du jour pour le detail des indices."
    try:
        from cache import read_cache

        meta_cache = read_cache("stock_ideas_meta") or {}
        regime_payload = (meta_cache.get("data") or {}).get("market_regime") or {}
        raw_regime = str(regime_payload.get("regime") or "").strip()
        market_score = regime_payload.get("score", "-")
        if raw_regime == "RISK_ON":
            market_regime = "Risk-on"
        elif raw_regime == "RISK_OFF":
            market_regime = "Risk-off"
        elif raw_regime:
            market_regime = raw_regime.replace("_", " ").title()
        reasons = regime_payload.get("reasons") or []
        if reasons:
            market_reason = str(reasons[0])
    except Exception:
        pass

    st.html(
        f"""
        <section class="app-hero">
            <div class="app-hero-grid">
                <div>
                    <div class="app-hero-kicker">Dashboard finance</div>
                    <h1 class="app-hero-title">Comparateur Boursier Interactif</h1>
                    <div class="app-hero-copy">
                        Compare, suis ton portefeuille et repere rapidement les signaux utiles avant d'agir.
                    </div>
                </div>
                <aside class="app-market-panel">
                    <div class="app-market-title">Contexte marche</div>
                    <div class="app-market-status">{escape(market_regime)}</div>
                    <div class="app-market-reason">{escape(market_reason)}</div>
                    <div class="app-market-grid">
                        <div class="app-market-chip">
                            <div class="app-market-chip-label">Score regime</div>
                            <div class="app-market-chip-value">{escape(str(market_score))}</div>
                        </div>
                        <div class="app-market-chip">
                            <div class="app-market-chip-label">Indices cles</div>
                            <div class="app-market-chip-value">S&P 500 / Nasdaq</div>
                        </div>
                    </div>
                </aside>
            </div>
        </section>
        """
    )

    last_refresh = time.strftime("%d/%m/%Y %H:%M", time.localtime(cache_path.stat().st_mtime))
    st.html(
        f"""
        <div class="app-system-strip">
            <span class="app-system-pill">{escape(f"{len(catalog):,}".replace(",", " "))} actifs couverts</span>
            <span class="app-system-pill">Annuaire mis a jour : {escape(last_refresh)}</span>
            <span class="app-system-pill">Sources : Nasdaq Trader, Yahoo Finance, CoinGecko, changedz.fr</span>
        </div>
        """
    )


def render_company_profile_section(catalog: pd.DataFrame) -> None:
    st.subheader("Fiche entreprise")
    st.caption("Recherche une entreprise cotee pour lire rapidement valorisation, croissance, risque, ratios et actualites.")

    companies = catalog[catalog["asset_type"] == "Entreprise"].copy()
    if companies.empty:
        st.info("Aucune entreprise n'est disponible dans le catalogue.")
        return

    companies = companies.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    label_by_ticker = dict(zip(companies["ticker"], companies["label"]))
    default_ticker = DEFAULT_TICKERS[0] if DEFAULT_TICKERS[0] in label_by_ticker else str(companies.iloc[0]["ticker"])

    if "company_profile_selected_ticker" not in st.session_state:
        st.session_state["company_profile_selected_ticker"] = default_ticker
    if "company_profile_search_version" not in st.session_state:
        st.session_state["company_profile_search_version"] = 0

    search_widget_key = f"company_profile_search_{st.session_state['company_profile_search_version']}"
    selected_from_search = st.selectbox(
        "Chercher une entreprise ou un ticker",
        options=companies["ticker"].tolist(),
        index=None,
        format_func=lambda ticker: label_by_ticker.get(ticker, ticker),
        key=search_widget_key,
        placeholder="Exemple: Apple, AAPL, Microsoft, NVDA...",
        help="La recherche se vide automatiquement apres la selection.",
    )
    if selected_from_search is not None:
        st.session_state["company_profile_selected_ticker"] = selected_from_search
        st.session_state["company_profile_search_version"] += 1
        st.rerun()

    selected_ticker = st.session_state.get("company_profile_selected_ticker", default_ticker)

    selected_company = companies[companies["ticker"] == selected_ticker].iloc[0]

    with st.spinner("Je charge la fiche entreprise..."):
        try:
            snapshot = fetch_company_snapshot(str(selected_ticker))
        except Exception as exc:  # pragma: no cover - depends on network/provider
            st.info(f"Impossible de charger cette fiche pour le moment : {exc}")
            return

    with st.container(border=True):
        top_col, sentiment_col = st.columns([2, 1])
        top_col.markdown(f"### {snapshot['name']}")
        top_col.caption(f"{snapshot['ticker']} | {selected_company['exchange'] or '-'}")

        sentiment_color = {
            "Positif": "#15803d",
            "Mitige": "#a16207",
            "Negatif": "#b91c1c",
        }.get(snapshot["sentiment_label"], "#334155")
        sentiment_col.html(
            f"<div style='padding-top:12px;text-align:right;'><span style='display:inline-block;padding:8px 12px;border-radius:999px;background:{sentiment_color};color:#fff;font-weight:700;'>{escape(snapshot['sentiment_label'])}</span></div>"
        )

        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        metric_col1.metric("CA annuel", format_money(snapshot["revenue"]))
        metric_col2.metric("Cours", f"{snapshot['last_price']:.2f}" if snapshot["last_price"] is not None else "-")
        metric_col3.metric("Variation 1 mois", f"{snapshot['month_change']:+.2f}%" if snapshot["month_change"] is not None else "-")
        metric_col4.metric("Capitalisation", format_money(snapshot["market_cap"]))

        st.markdown("#### Lecture rapide")
        quick_col1, quick_col2, quick_col3 = st.columns(3)
        quick_col1.metric("Valorisation", snapshot["valuation_label"])
        quick_col1.caption(snapshot["valuation_reason"])
        quick_col2.metric("Croissance", snapshot["growth_label"])
        quick_col2.caption(snapshot["growth_reason"])
        quick_col3.metric("Risque", snapshot["risk_label"])
        quick_col3.caption(snapshot["risk_reason"])

        info_col1, info_col2, info_col3 = st.columns(3)
        info_col1.write(f"**Secteur** : {snapshot['sector']}")
        info_col1.write(f"**Activite** : {snapshot['industry']}")
        info_col1.write(f"**Prochains resultats** : {format_optional_date(snapshot['next_earnings'])}")
        info_col2.write(f"**Sentiment marche** : {snapshot['sentiment_label']}")
        info_col2.caption(snapshot["sentiment_reason"])
        info_col3.write(f"**Objectif analystes** : {format_money(snapshot['target_price'])}")
        if snapshot["analyst_count"]:
            info_col3.caption(f"{int(snapshot['analyst_count'])} analyste(s) dans la source Yahoo Finance.")

        st.write(snapshot["summary"])

        st.markdown("#### Ratios et fondamentaux")
        ratios = pd.DataFrame(
            [
                {"Indicateur": "PER", "Valeur": format_ratio(snapshot["trailing_pe"])},
                {"Indicateur": "Price/Sales", "Valeur": format_ratio(snapshot["price_to_sales"])},
                {"Indicateur": "EV/EBITDA", "Valeur": format_ratio(snapshot["ev_to_ebitda"])},
                {"Indicateur": "Marge brute", "Valeur": format_decimal_percent(snapshot["gross_margin"])},
                {"Indicateur": "Marge operationnelle", "Valeur": format_decimal_percent(snapshot["operating_margin"])},
                {"Indicateur": "Croissance CA", "Valeur": format_decimal_percent(snapshot["revenue_growth"], signed=True)},
                {"Indicateur": "Croissance BPA", "Valeur": format_decimal_percent(snapshot["earnings_growth"], signed=True)},
                {"Indicateur": "Dette / Equity", "Valeur": format_ratio(snapshot["debt_to_equity"], suffix="")},
                {"Indicateur": "Dette totale", "Valeur": format_money(snapshot["total_debt"])},
                {"Indicateur": "Cash", "Valeur": format_money(snapshot["total_cash"])},
                {"Indicateur": "Free cash flow", "Valeur": format_money(snapshot["free_cashflow"])},
                {"Indicateur": "Dividende", "Valeur": format_decimal_percent(snapshot["dividend_yield"])},
            ]
        )
        st.dataframe(ratios, width="stretch", hide_index=True)

        if snapshot["peer_tickers"]:
            with st.expander("Comparaison rapide avec des pairs"):
                peer_options = [snapshot["ticker"]] + [ticker for ticker in snapshot["peer_tickers"] if ticker != snapshot["ticker"]]
                selected_peers = st.multiselect(
                    "Pairs a comparer",
                    options=peer_options,
                    default=peer_options[:4],
                    key=f"peers_{snapshot['ticker']}",
                )
                if selected_peers:
                    peer_frame = fetch_peer_comparison(tuple(selected_peers))
                    if peer_frame.empty:
                        st.info("Comparaison indisponible pour le moment.")
                    else:
                        display_peer_frame = peer_frame.copy()
                        for money_column in ("Capitalisation", "CA"):
                            display_peer_frame[money_column] = display_peer_frame[money_column].map(format_money)
                        for percent_column in ("Marge op.", "Croissance CA", "Perf 1 an"):
                            display_peer_frame[percent_column] = display_peer_frame[percent_column].map(
                                lambda value: format_decimal_percent(value, signed=True)
                            )
                        display_peer_frame["PER"] = display_peer_frame["PER"].map(format_ratio)
                        display_peer_frame["Dette/Equity"] = display_peer_frame["Dette/Equity"].map(
                            lambda value: format_ratio(value, suffix="")
                        )
                        st.dataframe(display_peer_frame, width="stretch", hide_index=True)

        if snapshot["news_items"]:
            st.caption("Dernieres infos")
            for item in snapshot["news_items"]:
                title = item.get("title") or "Sans titre"
                url = item.get("url")
                if url:
                    st.markdown(f"- [{title}]({url})")
                else:
                    st.markdown(f"- {title}")


def render_portfolio_section(catalog: pd.DataFrame, current_user: sqlite3.Row) -> None:
    render_section_heading(
        "Portefeuille simule",
        "Dis simplement combien tu veux investir dans un actif. L'app calcule la quantite et affiche la performance.",
    )

    positions = list_portfolio_positions(current_user["username"])
    visible_assets = catalog[catalog["asset_type"].isin(["Entreprise", "Indice", "ETF", "Crypto"])].copy()
    visible_assets = visible_assets.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    label_by_ticker = dict(zip(visible_assets["ticker"], visible_assets["label"]))

    with st.expander("Acheter un actif", expanded=not positions):
        with st.form("add_portfolio_position_form", clear_on_submit=True):
            selection_col, amount_col, date_col = st.columns([2, 1, 1])
            selected_ticker = selection_col.selectbox(
                "Je veux acheter",
                options=visible_assets["ticker"].tolist(),
                index=None,
                format_func=lambda ticker: label_by_ticker.get(ticker, ticker),
                placeholder="Ex: NVIDIA, Apple, Bitcoin...",
            )
            investment_amount_value = amount_col.text_input(
                "Montant investi",
                placeholder="500",
                help="Exemple : 500 pour acheter environ 500 dollars/euros de cet actif.",
            )
            purchase_date = date_col.date_input("Date", value=datetime.now().date())

            submitted = st.form_submit_button("Acheter virtuellement")

        if submitted:
            ticker = normalize_ticker(selected_ticker or "")
            investment_amount = parse_float_value(investment_amount_value)
            errors = []
            if not ticker:
                errors.append("Choisis un actif.")
            if investment_amount is None or investment_amount <= 0:
                errors.append("Montant investi invalide.")
            purchase_price = None
            if ticker:
                with st.spinner("Je cherche le prix d'achat de reference..."):
                    purchase_price = fetch_reference_purchase_price(ticker, purchase_date.isoformat())
            if purchase_price is None or purchase_price <= 0:
                errors.append("Impossible de trouver un prix d'achat pour cet actif et cette date.")
            if errors:
                render_validation_errors(errors)
            else:
                quantity = investment_amount / purchase_price
                metadata = fetch_ticker_metadata(ticker)
                try:
                    add_portfolio_position(
                        current_user["username"],
                        ticker,
                        quantity,
                        purchase_price,
                        purchase_date.isoformat(),
                        metadata.get("currency") or "",
                        f"Achat virtuel de {investment_amount:g}",
                    )
                    st.success(
                        f"Achat virtuel ajoute : {investment_amount:g} investis dans {ticker}, "
                        f"soit environ {quantity:.6g} titre(s) a {purchase_price:.2f}."
                    )
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))

    if not positions:
        st.info("Ajoute une premiere position pour activer le suivi de portefeuille.")
        return

    with st.spinner("Je mets a jour les cours du portefeuille..."):
        portfolio = build_portfolio_frame(positions, catalog)

    if portfolio.empty:
        st.info("Aucune position exploitable pour le moment.")
        return

    total_cost = portfolio["PRU total"].sum()
    priced_positions = portfolio.dropna(subset=["Valeur actuelle"]).copy()
    priced_cost = priced_positions["PRU total"].sum()
    total_value = priced_positions["Valeur actuelle"].sum()
    total_pnl = priced_positions["PnL latent"].sum()
    total_return = (total_pnl / priced_cost * 100) if priced_cost else 0.0
    missing_quotes = portfolio["Dernier cours"].isna().sum()
    best_position = priced_positions.sort_values("PnL latent (%)", ascending=False).head(1)
    worst_position = priced_positions.sort_values("PnL latent (%)", ascending=True).head(1)
    best_label = "-"
    worst_label = "-"
    if not best_position.empty:
        best_row = best_position.iloc[0]
        best_label = f"{best_row['Ticker']} {best_row['PnL latent (%)']:+.1f}%"
    if not worst_position.empty:
        worst_row = worst_position.iloc[0]
        worst_label = f"{worst_row['Ticker']} {worst_row['PnL latent (%)']:+.1f}%"
    render_summary_strip(
        "Decision portefeuille",
        [
            {"label": "Valeur totale", "value": format_money(total_value), "hint": "positions cotees", "tone": "info"},
            {"label": "Performance", "value": format_percent(total_return), "hint": "PnL latent total", "tone": "success" if total_return >= 0 else "danger"},
            {"label": "PnL absolu", "value": format_money(total_pnl), "hint": "gain ou perte latent", "tone": "success" if total_pnl >= 0 else "danger"},
            {"label": "Meilleure position", "value": best_label, "hint": "plus forte ligne", "tone": "success"},
            {"label": "Pire position", "value": worst_label, "hint": "ligne a surveiller", "tone": "danger" if worst_label != "-" else "warning"},
        ],
    )

    render_section_heading("Donnees secondaires", "Details utiles sans repeter la valeur, la performance ou les extremes du portefeuille.")
    metric_col1, metric_col2 = st.columns(2)
    metric_col1.metric("Capital investi", format_money(total_cost))
    metric_col2.metric("Lignes suivies", len(portfolio), delta=f"{missing_quotes} sans cotation" if missing_quotes else None)
    if missing_quotes:
        st.caption("Les positions sans cotation recente restent visibles, mais elles sont exclues du PnL et de la performance totale.")

    display_portfolio = portfolio[
        [
            "Nom",
            "Ticker",
            "PRU total",
            "Valeur actuelle",
            "Dernier cours",
            "PnL latent (%)",
            "PnL latent",
        ]
    ].copy()
    display_portfolio = display_portfolio.rename(
        columns={
            "PRU total": "Montant investi",
            "Dernier cours": "Prix de l'action",
            "PnL latent (%)": "PnL (%)",
            "PnL latent": "PnL ($)",
        }
    )

    def color_pnl(val):
        if pd.isna(val): return None
        return "color: #15803d" if val >= 0 else "color: #b91c1c"

    # Utilisation de .map() au lieu de .applymap() pour la compatibilité Pandas 2.1+
    styled_portfolio = display_portfolio.style.map(color_pnl, subset=["PnL (%)", "PnL ($)"])

    st.dataframe(
        styled_portfolio,
        width="stretch",
        hide_index=True,
        column_config={
            "PnL (%)": st.column_config.NumberColumn("PnL (%)", format="%.2f%%"),
            "Prix de l'action": st.column_config.NumberColumn("Prix de l'action", format="%.2f"),
            "Montant investi": st.column_config.NumberColumn("Montant investi", format="%.2f"),
            "Valeur actuelle": st.column_config.NumberColumn("Valeur actuelle", format="%.2f"),
            "PnL ($)": st.column_config.NumberColumn("PnL ($)", format="%.2f"),
        },
    )

    with st.spinner("Je reconstruis l'evolution du portefeuille..."):
        portfolio_history = build_portfolio_performance_history(portfolio)
    if not portfolio_history.empty:
        st.plotly_chart(build_portfolio_performance_figure(portfolio_history), width="stretch")
    else:
        st.info("Le graphe d'evolution apparaitra des que l'historique de prix sera disponible.")

    action_col1, action_col2 = st.columns([2, 1])
    position_to_delete = action_col1.selectbox(
        "Ligne a supprimer",
        options=portfolio["ID"].tolist(),
        format_func=lambda position_id: (
            f"{portfolio.loc[portfolio['ID'] == position_id, 'Ticker'].iloc[0]} | "
            f"{portfolio.loc[portfolio['ID'] == position_id, 'Date achat'].iloc[0]} | "
            f"{portfolio.loc[portfolio['ID'] == position_id, 'Quantite'].iloc[0]:g}"
        ),
        key="portfolio_delete_select",
    )
    if action_col2.button("Supprimer la ligne", use_container_width=True):
        delete_portfolio_position(current_user["username"], int(position_to_delete))
        st.success("Ligne supprimee.")
        st.rerun()

    allocation_tab, contribution_tab, benchmark_tab = st.tabs(["Allocation", "Contribution", "Benchmark"])
    with allocation_tab:
        group_choice = st.selectbox(
            "Regrouper par",
            options=["Type", "Secteur", "Devise"],
            key="portfolio_allocation_group",
        )
        allocation = build_allocation_frame(portfolio, group_choice)
        if allocation.empty:
            st.info("Allocation indisponible tant que les cotations ne sont pas chargees.")
        else:
            chart_col, table_col = st.columns([1, 1])
            chart_col.plotly_chart(build_allocation_figure(allocation, group_choice), width="stretch")
            table_col.dataframe(
                allocation,
                width="stretch",
                hide_index=True,
                column_config={
                    "Valeur actuelle": st.column_config.NumberColumn("Valeur actuelle", format="%.2f"),
                    "Poids (%)": st.column_config.NumberColumn("Poids (%)", format="%.2f%%"),
                },
            )

    with contribution_tab:
        contribution = portfolio[["Ticker", "Nom", "PnL latent", "Contribution perf (%)"]].copy()
        contribution = contribution.sort_values("PnL latent", ascending=False, na_position="last")
        fig = go.Figure(
            go.Bar(
                x=contribution["Ticker"],
                y=contribution["PnL latent"].fillna(0),
                marker_color=["#15803d" if value >= 0 else "#b91c1c" for value in contribution["PnL latent"].fillna(0)],
                hovertemplate="%{x}<br>PnL : %{y:.2f}<extra></extra>",
            )
        )
        fig.update_layout(
            template="plotly_white",
            title="Contribution absolue au PnL latent",
            xaxis_title="Position",
            yaxis_title="PnL latent",
            margin=dict(l=30, r=30, t=60, b=30),
        )
        st.plotly_chart(fig, width="stretch")
        st.dataframe(
            contribution,
            width="stretch",
            hide_index=True,
            column_config={
                "PnL latent": st.column_config.NumberColumn("PnL latent", format="%.2f"),
                "Contribution perf (%)": st.column_config.NumberColumn("Contribution perf (%)", format="%.2f%%"),
            },
        )

    with benchmark_tab:
        earliest_date = str(portfolio["Date achat"].min())
        benchmark = fetch_benchmark_returns(earliest_date)
        st.caption(
            "Comparaison simple depuis la premiere date d'achat du portefeuille. "
            "Elle ne remplace pas un calcul money-weighted avec flux exacts, mais donne un repere utile."
        )
        if benchmark.empty:
            st.info("Benchmarks indisponibles pour le moment.")
        else:
            benchmark = benchmark.copy()
            benchmark.loc[len(benchmark)] = {
                "Benchmark": "Portefeuille",
                "Ticker": "-",
                "Performance depuis debut (%)": round(float(total_return), 2),
            }
            st.dataframe(benchmark, width="stretch", hide_index=True)


def render_market_today_section(catalog: pd.DataFrame) -> None:
    render_section_heading(
        "Marche du jour",
        "Indices majeurs, stress de marche, taux, matieres premieres, crypto et dernieres nouvelles.",
    )

    if st.button("Rafraichir le briefing marche", key="refresh_market_today"):
        download_price_histories.clear()
        fetch_market_movers.clear()
        fetch_news_for_tickers.clear()

    tickers = tuple(row["ticker"] for row in MARKET_BRIEFING_ASSETS)
    with st.spinner("Je charge le briefing marche..."):
        try:
            price_history, return_history = download_price_histories(tickers=tickers, period="1mo", interval="1d")
            snapshot = build_market_snapshot_table(MARKET_BRIEFING_ASSETS, price_history, return_history)
        except Exception as exc:
            st.info(f"Briefing marche indisponible pour le moment : {exc}")
            snapshot = pd.DataFrame()

    if not snapshot.empty:
        index_rows = snapshot[snapshot["Groupe"] == "Indices"].copy()
        index_return = pd.to_numeric(index_rows.get("1j", pd.Series(dtype=float)), errors="coerce")
        avg_index_return = float(index_return.dropna().mean()) if not index_return.dropna().empty else 0.0
        market_sentiment = "Risk-on" if avg_index_return > 0.35 else "Risk-off" if avg_index_return < -0.35 else "Neutre"

        def market_value(asset_name: str, column: str) -> str:
            match = snapshot[snapshot["Actif"] == asset_name]
            if match.empty or column not in match.columns:
                return "-"
            value = match.iloc[0][column]
            if column in {"1j", "1m"}:
                return format_percent(value)
            return f"{value:.2f}" if isinstance(value, (int, float)) and not pd.isna(value) else str(value)

        render_summary_strip(
            "Lecture marche",
            [
                {"label": "S&P 500", "value": market_value("S&P 500", "1j"), "hint": "variation 1 jour", "tone": "success" if market_value("S&P 500", "1j").startswith("+") else "danger" if market_value("S&P 500", "1j").startswith("-") else "warning"},
                {"label": "Nasdaq", "value": market_value("Nasdaq", "1j"), "hint": "variation 1 jour", "tone": "success" if market_value("Nasdaq", "1j").startswith("+") else "danger" if market_value("Nasdaq", "1j").startswith("-") else "warning"},
                {"label": "Sentiment global", "value": market_sentiment, "hint": "moyenne indices", "tone": "success" if market_sentiment == "Risk-on" else "danger" if market_sentiment == "Risk-off" else "warning"},
                {"label": "VIX", "value": market_value("VIX", "Dernier"), "hint": "stress marche", "tone": "warning"},
            ],
        )

        international_index_rows = index_rows[~index_rows["Actif"].isin(["S&P 500", "Nasdaq"])].copy()
        if not international_index_rows.empty:
            render_section_heading("Indices internationaux", "Repere secondaire sans dupliquer les indices US du bloc principal.")
            metric_cols = st.columns(min(4, max(1, len(international_index_rows))))
            for col, (_, row) in zip(metric_cols, international_index_rows.iterrows()):
                col.metric(row["Actif"], row["Dernier"], delta=format_percent(row["1j"]))

            st.dataframe(
                international_index_rows,
                width="stretch",
                hide_index=True,
                column_config={
                    "Dernier": st.column_config.NumberColumn("Dernier", format="%.2f"),
                    "1j": st.column_config.NumberColumn("1j", format="%.2f%%"),
                    "1m": st.column_config.NumberColumn("1m", format="%.2f%%"),
                },
            )

    mover_col, news_col = st.columns([1, 1])
    with mover_col:
        render_market_movers_section(catalog)

    with news_col:
        render_section_heading("News cles", "Derniers titres relies aux actifs de contexte.")
        try:
            news_items = fetch_news_for_tickers(("^GSPC", "^FCHI", "BTC-USD", "GC=F"), per_ticker_limit=4)
        except Exception:
            news_items = []
        if not news_items:
            st.info("Aucune news marche disponible pour le moment.")
        else:
            for item in news_items[:6]:
                title = item.get("title") or "Sans titre"
                provider = item.get("provider") or "Source inconnue"
                published = format_news_datetime(item.get("published_at"))
                url = item.get("url")
                with st.container(border=True):
                    if url:
                        st.markdown(f"**[{title}]({url})**")
                    else:
                        st.markdown(f"**{title}**")
                    st.caption(f"{item.get('ticker')} | {provider} | {published}")


def render_podcast_briefing_controls(current_user: sqlite3.Row) -> None:
    if current_user["role"] != "admin":
        return

    with st.expander("Podcast briefing audio"):
        has_openai_key = bool(get_openai_api_key())
        local_engines = local_tts_engines()
        if not has_openai_key and not local_engines:
            st.warning(
                "OPENAI_API_KEY n'est pas configuree et aucun moteur TTS local n'est disponible. "
                "Le script peut etre genere, mais l'audio restera indisponible."
            )

        default_categories = [category for category in ("A la une", "Economie", "International") if category in GENERAL_NEWS_FEEDS]
        category_col, duration_col, tone_col = st.columns([2, 1, 1])
        selected_categories = category_col.multiselect(
            "Rubriques du podcast",
            options=list(GENERAL_NEWS_FEEDS.keys()),
            default=default_categories,
            key="podcast_categories",
        )
        duration_minutes = duration_col.selectbox(
            "Duree cible",
            options=PODCAST_DURATION_OPTIONS,
            index=PODCAST_DURATION_OPTIONS.index(PODCAST_DEFAULT_DURATION_MINUTES),
            format_func=lambda value: f"{value} min",
            key="podcast_duration",
        )
        tone = tone_col.selectbox(
            "Ton",
            options=[
                "generaliste, clair, pose et professionnel",
                "generaliste, dynamique mais serieux",
                "sobre et tres synthetique",
            ],
            key="podcast_tone",
        )
        include_portfolio = True
        include_market_context = False
        force_digest_rebuild = False

        if st.button("Generer le script du podcast", use_container_width=True):
            with st.spinner("Je collecte les infos, dedoublonne les sujets et prepare le script..."):
                try:
                    context = collect_podcast_briefing_context(
                        current_user,
                        selected_categories,
                        include_portfolio=include_portfolio,
                        include_market_context=include_market_context,
                        force_digest_rebuild=force_digest_rebuild,
                    )
                    script, generation_mode = generate_podcast_script(context, duration_minutes, tone)
                    output_dir = get_briefing_output_dir(datetime.now().strftime("%Y-%m-%d-%H%M%S"))
                    save_podcast_assets(context, script, output_dir)
                except Exception as exc:
                    st.error(f"Impossible de generer le script : {exc}")
                else:
                    st.session_state["podcast_script"] = script
                    st.session_state["podcast_script_editor"] = script
                    st.session_state["podcast_context"] = context
                    st.session_state["podcast_output_dir"] = str(output_dir)
                    st.session_state["podcast_audio_path"] = ""
                    st.success(
                        "Script genere avec OpenAI." if generation_mode == "openai" else "Script genere en mode fallback."
                    )

        digest = (st.session_state.get("podcast_context") or {}).get("news_digest") or {}
        if digest:
            reused_label = "reutilise" if digest.get("reused") else "reconstruit"
            main_title = (digest.get("main_topic") or {}).get("main_title") or "-"
            st.caption(
                f"Digest editorial {reused_label} · {digest.get('digest_id', '-')} · "
                f"sujet principal : {main_title}"
            )
            with st.expander("Debug digest editorial"):
                policy = digest.get("selection_policy") or {}
                st.write(
                    {
                        "digest_id": digest.get("digest_id"),
                        "generated_at": digest.get("generated_at"),
                        "reused": digest.get("reused"),
                        "main_reason": policy.get("main_reason"),
                        "major_override_check": digest.get("major_override_check"),
                        "raw_article_count": digest.get("raw_article_count"),
                        "candidate_cluster_count": digest.get("candidate_cluster_count"),
                    }
                )
                rows = []
                for role, cluster in [("principal", digest.get("main_topic") or {})] + [
                    (f"secondaire {idx}", cluster)
                    for idx, cluster in enumerate(digest.get("secondary_topics") or [], start=1)
                ]:
                    if not cluster:
                        continue
                    rows.append(
                        {
                            "Role": role,
                            "Sujet": cluster.get("main_title"),
                            "Score": cluster.get("score"),
                            "Articles": cluster.get("article_count"),
                            "Sources": cluster.get("source_count"),
                            "Pourquoi": ", ".join(cluster.get("why_selected", [])),
                        }
                    )
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        script_value = st.session_state.get("podcast_script", "")
        if script_value:
            edited_script = st.text_area(
                "Script a lire",
                value=script_value,
                height=420,
                key="podcast_script_editor",
            )
            st.session_state["podcast_script"] = edited_script

            audio_engine_options = []
            if has_openai_key:
                audio_engine_options.append("OpenAI TTS")
            audio_engine_options.extend(local_engines.keys())
            if not audio_engine_options:
                audio_engine_options = ["Indisponible"]
            engine_col, voice_col, instruction_col = st.columns([1, 1, 2])
            selected_engine = engine_col.selectbox(
                "Moteur audio",
                options=audio_engine_options,
                key="podcast_audio_engine",
            )
            if selected_engine == "OpenAI TTS":
                voice = voice_col.selectbox(
                    "Voix",
                    options=["alloy", "verse", "coral", "nova", "sage"],
                    index=0,
                    key="podcast_voice",
                )
                voice_instructions = instruction_col.text_input(
                    "Direction vocale",
                    value="Voix francaise naturelle, rythme soutenu et dynamique, ton briefing radio professionnel.",
                    key="podcast_voice_instructions",
                )
            else:
                voice = ""
                voice_instructions = ""
                voice_col.caption("Voix locale fr-fr")
                instruction_col.caption(
                    "Le moteur local fonctionne sans API, mais la voix est plus robotique qu'OpenAI TTS."
                )

            if st.button(
                "Generer l'audio",
                disabled=selected_engine == "Indisponible",
                use_container_width=True,
            ):
                with st.spinner("Je genere le fichier audio..."):
                    try:
                        output_dir = Path(st.session_state.get("podcast_output_dir") or get_briefing_output_dir())
                        audio_path = output_dir / "briefing.mp3"
                        if selected_engine == "OpenAI TTS":
                            final_audio_path = generate_podcast_audio_file(
                                edited_script,
                                audio_path,
                                voice,
                                voice_instructions,
                            )
                        else:
                            final_audio_path = generate_local_espeak_audio_file(
                                edited_script,
                                audio_path,
                                local_engines.get(selected_engine, ""),
                            )
                    except Exception as exc:
                        st.error(f"Impossible de generer l'audio : {exc}")
                    else:
                        st.session_state["podcast_audio_path"] = str(final_audio_path)
                        st.success(f"Audio genere avec {selected_engine}.")

        audio_path_value = st.session_state.get("podcast_audio_path", "")
        if audio_path_value:
            audio_path = Path(audio_path_value)
            if audio_path.exists():
                st.audio(str(audio_path))
                st.caption(f"Fichier audio : {audio_path}")

        old_podcasts = list_generated_podcasts()
        if old_podcasts:
            with st.expander("Anciens podcasts audio"):
                selected_podcast_label = st.selectbox(
                    "Podcast genere",
                    options=[podcast["folder"] for podcast in old_podcasts],
                    format_func=lambda folder: next(
                        (
                            f"{podcast['label']} · {podcast['size_mb']:.1f} Mo"
                            for podcast in old_podcasts
                            if podcast["folder"] == folder
                        ),
                        folder,
                    ),
                    key="podcast_history_select",
                )
                selected_podcast = next(
                    (podcast for podcast in old_podcasts if podcast["folder"] == selected_podcast_label),
                    old_podcasts[0],
                )
                st.audio(str(selected_podcast["audio_path"]))
                st.caption(f"Dossier : {selected_podcast['folder']} | Fichier : {selected_podcast['audio_path']}")
                if selected_podcast.get("script_path"):
                    with st.expander("Voir le script associe"):
                        try:
                            st.markdown(selected_podcast["script_path"].read_text(encoding="utf-8"))
                        except OSError:
                            st.info("Script indisponible pour ce podcast.")

        if st.session_state.get("podcast_script"):
            st.divider()
            email_col, btn_col = st.columns([3, 1])
            email_recipient = email_col.text_input(
                "Envoyer le briefing par email",
                value="rafik.mo1995@gmail.com",
                label_visibility="visible",
                key="briefing_email_recipient",
            )
            config_errors = validate_news_email_config(get_news_email_config(email_recipient))
            if btn_col.button(
                "Envoyer",
                use_container_width=True,
                disabled=bool(config_errors) or not email_recipient,
                key="briefing_email_send",
            ):
                script_text = st.session_state.get("podcast_script", "")
                subject = f"Briefing du {datetime.now().strftime('%d/%m/%Y')}"
                html_body = "<pre style='font-family:sans-serif;white-space:pre-wrap'>" + script_text.replace("&", "&amp;").replace("<", "&lt;") + "</pre>"
                with st.spinner("Envoi en cours..."):
                    try:
                        send_email_message(subject, script_text, html_body, recipients_override=email_recipient)
                    except Exception as exc:
                        st.error(f"Impossible d'envoyer : {exc}")
                    else:
                        st.success(f"Briefing envoye a {email_recipient}.")
            if config_errors:
                st.caption(f"SMTP non configure : {' '.join(config_errors)}")


def render_briefing_email_schedule(current_user: sqlite3.Row) -> None:
    if current_user["role"] != "admin":
        return

    schedule = get_email_schedule()

    with st.expander("Envoi automatique du briefing par email"):
        enabled = st.toggle("Activer l'envoi automatique", value=schedule["enabled"], key="bes_enabled")

        hours = [f"{h:02d}:00" for h in range(6, 23)]
        send_time = st.selectbox(
            "Heure d'envoi",
            options=hours,
            index=hours.index(schedule["send_time"]) if schedule["send_time"] in hours else hours.index("08:00"),
            key="bes_send_time",
        )

        st.caption("Destinataires")
        current_recipients = list(schedule["recipients"])
        to_remove = []
        for i, r in enumerate(current_recipients):
            col_email, col_del = st.columns([5, 1])
            col_email.text(r)
            if col_del.button("✕", key=f"bes_del_{i}"):
                to_remove.append(r)
        current_recipients = [r for r in current_recipients if r not in to_remove]

        new_email = st.text_input("Ajouter un destinataire", placeholder="adresse@example.com", key="bes_new_email")
        add_col, save_col = st.columns(2)
        if add_col.button("Ajouter", key="bes_add", use_container_width=True):
            if new_email and EMAIL_PATTERN.fullmatch(new_email.strip()):
                if new_email.strip() not in current_recipients:
                    current_recipients.append(new_email.strip())
                save_email_schedule(enabled, current_recipients, send_time)
                st.rerun()
            else:
                st.error("Adresse invalide.")

        if save_col.button("Enregistrer", key="bes_save", use_container_width=True):
            save_email_schedule(enabled, current_recipients, send_time)
            st.success("Paramètres enregistrés.")

        if schedule["last_sent_date"]:
            st.caption(f"Dernier envoi : {schedule['last_sent_date']}")

        st.caption(
            "Activer le cron (une seule fois sur le serveur) :  \n"
            "`* * * * * cd /root/Documents/streamlit_app && /root/Documents/streamlit_app/.venv/bin/python app.py send-briefing-email`"
        )


def render_news_section(catalog: pd.DataFrame, comparison_tickers: list[str], current_user: sqlite3.Row) -> None:
    general_tab, market_tab = st.tabs(["Infos generales", "News marche"])

    with market_tab:
        render_section_heading(
            "Actualites marche",
            "Flux recents par actif depuis Yahoo Finance. Utilise le bouton de rafraichissement pour recharger les news.",
        )

        if st.button("Rafraichir les actualites marche", key="refresh_news_button"):
            fetch_news_for_tickers.clear()

        available_tickers = [ticker for ticker in comparison_tickers if ticker]
        if not available_tickers:
            st.info("Ajoute des actifs au comparateur pour afficher leurs actualites.")
        else:
            filter_col, sort_col = st.columns(2)
            ticker_options = ["Tous"] + available_tickers
            selected_ticker = filter_col.selectbox("Filtrer par actif", options=ticker_options, key="news_ticker_filter")
            market_sort = sort_col.selectbox(
                "Trier les news marche",
                options=["Plus recentes", "Plus anciennes", "Source A-Z", "Actif A-Z", "Titre A-Z"],
                key="market_news_sort",
            )

            with st.spinner("Je recupere les actualites recentes..."):
                news_items = fetch_news_for_tickers(tuple(available_tickers))

            if selected_ticker != "Tous":
                news_items = [item for item in news_items if item["ticker"] == selected_ticker]

            news_items = sort_market_news_items(news_items, market_sort)

            if not news_items:
                st.info("Aucune actualite recente disponible pour la selection actuelle.")
            else:
                catalog_by_ticker = catalog.set_index("ticker")
                for item in news_items[:20]:
                    ticker = item["ticker"]
                    name = ticker
                    if ticker in catalog_by_ticker.index:
                        name = str(catalog_by_ticker.loc[ticker]["name"])

                    title = item["title"]
                    url = item["url"]
                    provider = item["provider"]
                    published = format_news_datetime(item["published_at"])
                    summary = item["summary"].strip()

                    with st.container(border=True):
                        st.markdown(f"#### {title}")
                        st.caption(f"{name} ({ticker}) | Source : {provider} | {published}")
                        if summary:
                            st.write(summary)
                        if url:
                            st.markdown(f"[Ouvrir l'article]({url})")

    with general_tab:
        render_section_heading(
            "Infos generales",
            "Actualites generalistes issues de plusieurs medias : presse nationale, chaines info et medias internationaux francophones.",
        )

        category = st.selectbox(
            "Rubrique",
            options=list(GENERAL_NEWS_FEEDS.keys()),
            key="general_news_category",
        )
        if st.button("Rafraichir les infos generales", key="refresh_general_news_button"):
            fetch_general_news.clear()

        render_podcast_briefing_controls(current_user)
        render_briefing_email_schedule(current_user)

        sort_col, source_col = st.columns(2)
        general_sort = sort_col.selectbox(
            "Trier les infos generales",
            options=["Plus recentes", "Plus anciennes", "Source A-Z", "Flux A-Z", "Titre A-Z"],
            key="general_news_sort",
        )
        source_filter = source_col.selectbox(
            "Filtrer par media",
            options=["Tous"] + [feed["label"] for feed in GENERAL_NEWS_FEEDS[category]],
            key="general_news_source_filter",
        )

        with st.spinner("Je recupere les infos generales..."):
            general_news = fetch_general_news(category)

        if source_filter != "Tous":
            general_news = [item for item in general_news if item["feed_label"] == source_filter]

        general_news = sort_general_news_items(general_news, general_sort)
        loaded_feeds = sorted({item["feed_label"] for item in general_news})

        if not general_news:
            st.info("Aucune actualite generale n'a pu etre chargee pour cette rubrique.")
            return

        st.caption(
            f"{len(general_news)} article(s) charges depuis {len(loaded_feeds)} media(s) pour la rubrique `{category}`."
        )

        for item in general_news[:25]:
            with st.container():
                render_general_news_card(item)


def render_comparator_section(
    catalog: pd.DataFrame,
    asset_types: list[str],
    smooth_closures: bool,
    use_log_scale: bool,
) -> list[str]:
    render_section_heading("Comparateur", "Compare plusieurs actions, indices, ETF ou cryptos avec les courbes de performance et de prix.")

    visible_catalog = catalog[catalog["asset_type"].isin(asset_types)].copy()
    if visible_catalog.empty:
        st.warning("Aucun actif ne correspond au filtre choisi.")
        return []

    default_labels = visible_catalog[visible_catalog["ticker"].isin(DEFAULT_TICKERS)]["label"].tolist()

    col_period, col_mode, col_prepost, col_search = st.columns([1, 1, 1.4, 4])
    selected_period_label = col_period.selectbox("Durée", options=list(PERIOD_OPTIONS.keys()), index=0)
    period_config = PERIOD_OPTIONS[selected_period_label]
    is_intraday_period = period_config["interval"].endswith("m") or period_config["interval"].endswith("h")
    advanced_mode = col_mode.toggle(
        "Mode avancé",
        value=False,
        help="Affiche les colonnes techniques, les prix regular/pre-post et tous les actifs selectionnes sur les graphes.",
        key="main_comparator_advanced_mode",
    )
    include_prepost = col_prepost.toggle(
        "Inclure pre/post",
        value=False,
        disabled=not is_intraday_period,
        help=(
            "Disponible uniquement sur les intervalles intraday Yahoo Finance. "
            "Quand actif, les graphes incluent explicitement les points pre-market et after-hours si Yahoo les fournit."
            if is_intraday_period
            else "Disponible uniquement sur les vues intraday Yahoo Finance."
        ),
        key="main_comparator_prepost",
    )
    selected_labels = col_search.multiselect(
        "Recherche et comparaison",
        options=visible_catalog["label"].tolist(),
        default=default_labels,
        max_selections=MAX_COMPARISON_COUNT,
        placeholder="Tape le nom d'une entreprise ou d'un indice, puis selectionne-le.",
        help="Tu peux chercher par nom d'entreprise, ticker ou nom d'indice.",
        key="main_comparator_assets",
    )

    if not selected_labels:
        st.info("Selectionne au moins un actif pour afficher les graphes.")
        return []

    selected_assets = visible_catalog.set_index("label").loc[selected_labels].reset_index().copy()
    selected_assets = selected_assets.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    comparison_tickers = selected_assets["ticker"].tolist()
    tickers = tuple(comparison_tickers)
    label_by_ticker = dict(zip(selected_assets["ticker"], selected_assets["name"]))

    with st.spinner("Je recupere les historiques de marche..."):
        try:
            price_history, return_history = download_price_histories(
                tickers=tickers,
                period=period_config["period"],
                interval=period_config["interval"],
                include_prepost=bool(include_prepost and is_intraday_period),
            )
            regular_price_history = price_history
            if include_prepost and is_intraday_period:
                regular_price_history, _ = download_price_histories(
                    tickers=tickers,
                    period=period_config["period"],
                    interval=period_config["interval"],
                    include_prepost=False,
                )
        except Exception as exc:  # pragma: no cover - depends on network/provider
            st.error(f"Impossible de recuperer les donnees de marche : {exc}")
            return comparison_tickers

    if price_history.empty:
        st.info("Aucune donnee de prix exploitable pour cette selection.")
        return comparison_tickers

    missing_tickers = [ticker for ticker in tickers if ticker not in price_history.columns]
    if missing_tickers:
        st.warning(f"Aucune donnee exploitable pour : {', '.join(missing_tickers)}")

    graph_tickers = comparison_tickers
    display_history = build_display_history(price_history, smooth_closures)
    display_return_history = build_display_history(return_history, smooth_closures)
    graph_display_history = display_history[[ticker for ticker in graph_tickers if ticker in display_history.columns]]
    graph_return_history = display_return_history[[ticker for ticker in graph_tickers if ticker in display_return_history.columns]]
    graph_performance = compute_performance_frame(graph_return_history)

    primary_ticker = graph_tickers[0] if graph_tickers else None
    price_figure = build_price_figure(graph_display_history, label_by_ticker, smooth_closures, primary_ticker=primary_ticker)
    if use_log_scale:
        price_figure.update_yaxes(type="log")

    summary_table = build_summary_table(
        selected_assets,
        price_history,
        return_history,
        regular_price_history=regular_price_history,
        include_prepost=bool(include_prepost and is_intraday_period),
    )
    leader_label = "-"
    laggard_label = "-"
    if not summary_table.empty and "Variation (%)" in summary_table.columns:
        variations = pd.to_numeric(summary_table["Variation (%)"], errors="coerce")
        if not variations.dropna().empty:
            leader = summary_table.loc[variations.idxmax()]
            laggard = summary_table.loc[variations.idxmin()]
            leader_label = f"{leader['Ticker']} {leader['Variation (%)']:+.1f}%"
            laggard_label = f"{laggard['Ticker']} {laggard['Variation (%)']:+.1f}%"
    render_summary_strip(
        "Lecture comparaison",
        [
            {"label": "Actifs compares", "value": len(comparison_tickers), "hint": "selection active", "tone": "info"},
            {"label": "Leader", "value": leader_label, "hint": "meilleure performance", "tone": "success"},
            {"label": "A surveiller", "value": laggard_label, "hint": "plus faible performance", "tone": "danger" if laggard_label != "-" else "warning"},
            {"label": "Periode", "value": selected_period_label, "hint": f"intervalle {period_config['interval']}", "tone": "info"},
        ],
    )

    render_section_heading("Vue d'ensemble", "Synthese rapide des prix, variations et sessions de marche.")
    simple_cols = ["Actif", "Region", "Prix", "Variation (%)", "Session"]
    advanced_cols = [
        "Actif", "Region", "Prix", "Variation (%)", "Session", "Source prix",
        "Prix regular", "Prix pre/post", "Prix affiche", "Dernier timestamp",
        "Marche", "Devise", "Type",
    ]
    display_cols = advanced_cols if advanced_mode else simple_cols
    display_cols = [col for col in display_cols if col in summary_table.columns]
    st.dataframe(
        summary_table[display_cols],
        width="stretch",
        hide_index=True,
        column_config={
            "Prix": st.column_config.NumberColumn("Prix", format="%.2f"),
            "Prix regular": st.column_config.Column("Prix regular"),
            "Prix pre/post": st.column_config.Column("Prix pre/post"),
            "Prix affiche": st.column_config.NumberColumn("Prix affiche", format="%.2f"),
            "Variation (%)": st.column_config.NumberColumn("Variation", format="%+.2f%%"),
            "Session": st.column_config.TextColumn("Session"),
            "Source prix": st.column_config.TextColumn("Source prix"),
        },
    )

    performance_tab, price_tab = st.tabs(["Performance (%)", "Prix"])
    with performance_tab:
        st.plotly_chart(
            build_performance_figure(graph_performance, label_by_ticker, smooth_closures, primary_ticker=primary_ticker),
            width="stretch",
        )
    with price_tab:
        st.plotly_chart(price_figure, width="stretch")

    st.caption(
        f"Periode chargee : {selected_period_label} | Intervalle Yahoo Finance : {period_config['interval']} | "
        "Les prix sont affiches dans leur devise de cotation d'origine. "
        + (
            "Les graphes sont lisses pendant les fermetures de marche, sans modifier les donnees brutes du tableau."
            if smooth_closures
            else "Les graphes suivent uniquement les points de cotation reels."
        )
        + " Les performances (%) sont calculees sur une serie ajustee quand elle est disponible. "
        + (
            "Pre/after-market inclus explicitement pour cette vue intraday."
            if include_prepost and is_intraday_period
            else "Pre/after-market non inclus dans cette vue."
        )
        + (
            " Mode simple : tableau reduit, graphes avec tous les actifs selectionnes."
            if not advanced_mode
            else " Mode avance : toutes les colonnes et tous les actifs selectionnes sont affiches."
        )
    )
    return comparison_tickers


def default_comparison_tickers(catalog: pd.DataFrame, asset_types: list[str]) -> list[str]:
    visible_catalog = catalog[catalog["asset_type"].isin(asset_types)].copy()
    if visible_catalog.empty:
        return []
    defaults = visible_catalog[visible_catalog["ticker"].isin(DEFAULT_TICKERS)]
    return defaults.drop_duplicates(subset=["ticker"])["ticker"].tolist()


def display_connection_logs() -> None:
    """Affiche les logs de connexion historiques."""
    st.title("📋 Logs de Connexion Historiques")
    st.caption("Accès public aux logs de connexion et d'authentification")
    
    init_user_db()
    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    
    try:
        # Récupérer les logs avec filtres optionnels
        col1, col2 = st.columns(2)
        with col1:
            event_filter = st.selectbox(
                "Filtrer par type d'événement",
                options=["Tous"] + list(set(
                    row[0] for row in conn.execute(
                        "SELECT DISTINCT event_type FROM auth_audit_log ORDER BY event_type"
                    ).fetchall()
                )),
                key="event_filter"
            )
        with col2:
            days_back = st.slider("Jours passés", 1, 90, 7, key="days_back")
        
        # Requête SQL
        query = "SELECT * FROM auth_audit_log WHERE 1=1"
        params = []
        
        if event_filter != "Tous":
            query += " AND event_type = ?"
            params.append(event_filter)
        
        if days_back:
            query += f" AND occurred_at >= datetime('now', '-{days_back} days')"
        
        query += " ORDER BY occurred_at DESC LIMIT 1000"
        
        logs = conn.execute(query, params).fetchall()
        
        if logs:
            # Afficher sous forme de dataframe
            logs_data = []
            for log in logs:
                logs_data.append({
                    "Date/Heure": log["occurred_at"],
                    "Type d'événement": log["event_type"],
                    "Acteur": log["actor_username"],
                    "Utilisateur cible": log["target_username"],
                    "Détails": log["details"],
                })
            
            df = pd.DataFrame(logs_data)
            st.dataframe(df, use_container_width=True, height=400)
            
            # Statistiques
            st.subheader("📊 Statistiques")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total logs", len(logs))
            with col2:
                logins = sum(1 for log in logs if log["event_type"] == "login")
                st.metric("Connexions", logins)
            with col3:
                logouts = sum(1 for log in logs if log["event_type"] == "logout")
                st.metric("Déconnexions", logouts)
            with col4:
                failed_attempts = sum(1 for log in logs if "failed" in (log["event_type"] or "").lower())
                st.metric("Échecs", failed_attempts)
        else:
            st.info("Aucun log disponible pour cette période")
    finally:
        conn.close()


_APP_PWD_TOKEN = "rafik_verified_v1"


def check_password() -> bool:
    if st.session_state.get("password_verified"):
        return True

    if st.query_params.get("_auth") == _APP_PWD_TOKEN:
        st.session_state.password_verified = True
        return True

    st.title("Acces Protege")
    with st.form("password_form"):
        password = st.text_input("Mot de passe", type="password", key="password_input")
        submitted = st.form_submit_button("Acceder")

    if submitted:
        if password == "rafik":
            st.query_params["_auth"] = _APP_PWD_TOKEN
            st.session_state.password_verified = True
            st.rerun()
        else:
            st.error("Mot de passe incorrect.")

    return False


def main() -> None:
    init_user_db()
    if not check_password():
        return

    render_auth_cookie_sync()

    current_user = {
        "username": "rafik",
        "display_name": "Rafik",
        "role": "admin",
        "must_change_password": False
    }

    ready, reason = market_data_is_ready()
    if not ready:
        st.error(reason)
        return

    # Parametres par defaut (sidebar supprimee pour epurer l'interface)
    refresh_catalog = False
    refresh_crypto = False
    asset_types = ["Entreprise", "Indice", "Crypto"]
    smooth_closures = True
    use_log_scale = False

    try:
        cache_path = download_company_directory(force_refresh=refresh_catalog)
        crypto_cache_path = download_crypto_directory(force_refresh=refresh_crypto)
    except Exception as exc:  # pragma: no cover - depends on network/provider
        st.error(f"Impossible de telecharger l'annuaire des tickers : {exc}")
        return

    catalog = load_symbol_catalog(max(cache_path.stat().st_mtime, crypto_cache_path.stat().st_mtime))
    render_header(catalog, cache_path)
    page_names = ["Comparateur", "Portefeuille", "Marche du jour", "Change", "Analyse", "Actualites"]
    if st.session_state.get("main_page") not in page_names:
        st.session_state["main_page"] = page_names[0]
    selected_page = st.radio(
        "Page",
        options=page_names,
        horizontal=True,
        key="main_page",
        label_visibility="collapsed",
    )

    comparison_tickers = st.session_state.get("comparison_tickers") or default_comparison_tickers(catalog, asset_types)

    if selected_page == "Comparateur":
        comparison_tickers = render_comparator_section(
            catalog,
            asset_types,
            smooth_closures,
            use_log_scale,
        )
        st.session_state["comparison_tickers"] = comparison_tickers

    elif selected_page == "Marche du jour":
        render_market_today_section(catalog)

    elif selected_page == "Change":
        render_change_rates_section(USER_AGENT)

    elif selected_page == "Portefeuille":
        render_portfolio_section(catalog, current_user)

    elif selected_page == "Analyse":
        render_midcap_recommendations_section()

    elif selected_page == "Actualites":
        render_news_section(catalog, comparison_tickers, current_user)


def run_daily_news_email_command() -> int:
    init_user_db()
    schedule = get_email_schedule()
    if not schedule["enabled"]:
        print("Envoi automatique desactive. Rien a faire.")
        return 0
    try:
        result = send_daily_news_recap_email(recipients_override=", ".join(schedule["recipients"]))
    except Exception as exc:
        print(f"Erreur envoi recap infos : {exc}", file=sys.stderr)
        return 1
    print(f"Recap infos envoye a {result['sent_count']} destinataire(s) ({schedule['recipients']}).")
    return 0


def run_send_briefing_email_command() -> int:
    init_user_db()
    schedule = get_email_schedule()
    if not schedule["enabled"]:
        print("Envoi automatique briefing desactive.")
        return 0

    now = datetime.now()
    current_time = now.strftime("%H:%M")
    today = now.strftime("%Y-%m-%d")

    if current_time != schedule["send_time"]:
        return 0

    if schedule["last_sent_date"] == today:
        print(f"Briefing deja envoye aujourd'hui ({today}).")
        return 0

    recipients = schedule["recipients"]
    if not recipients:
        print("Aucun destinataire configure.", file=sys.stderr)
        return 1

    try:
        categories = ["A la une", "Economie", "International"]
        context = collect_podcast_briefing_context(None, categories, include_portfolio=False)
        script, _ = generate_podcast_script(context, duration_minutes=5, tone="generaliste, clair, pose et professionnel")
    except Exception as exc:
        print(f"Erreur generation script : {exc}", file=sys.stderr)
        return 1

    subject = f"Briefing IA du {now.strftime('%d/%m/%Y')}"
    html_body = "<pre style='font-family:sans-serif;white-space:pre-wrap'>" + script.replace("&", "&amp;").replace("<", "&lt;") + "</pre>"
    errors = []
    for recipient in recipients:
        try:
            send_email_message(subject, script, html_body, recipients_override=recipient)
            print(f"Briefing envoye a {recipient}.")
        except Exception as exc:
            print(f"Erreur envoi a {recipient} : {exc}", file=sys.stderr)
            errors.append(recipient)

    if not errors:
        mark_briefing_email_sent_today()
    return 1 if errors else 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "send-daily-news-email":
        raise SystemExit(run_daily_news_email_command())
    if len(sys.argv) > 1 and sys.argv[1] == "send-briefing-email":
        raise SystemExit(run_send_briefing_email_command())
    main()
