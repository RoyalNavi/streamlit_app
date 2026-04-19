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

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - optional dependency at runtime
    yf = None


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
BRIEFINGS_DIR = DATA_DIR / "briefings"
MARKET_CACHE_PATH = DATA_DIR / "market_directory.csv"
CRYPTO_CACHE_PATH = DATA_DIR / "crypto_directory.csv"
MARKET_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
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

PERIOD_OPTIONS = {
    "1 jour": {"period": "1d", "interval": "5m"},
    "5 jours": {"period": "5d", "interval": "30m"},
    "1 mois": {"period": "1mo", "interval": "1d"},
    "3 mois": {"period": "3mo", "interval": "1d"},
    "6 mois": {"period": "6mo", "interval": "1d"},
    "1 an": {"period": "1y", "interval": "1d"},
    "2 ans": {"period": "2y", "interval": "1wk"},
    "5 ans": {"period": "5y", "interval": "1wk"},
    "Maximum": {"period": "max", "interval": "1mo"},
}

DEFAULT_TICKERS = ["^FCHI", "^GDAXI", "^GSPC", "URTH", "^IXIC"]
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
        {"label": "France 24 - Economie", "url": "https://www.france24.com/fr/%C3%A9co-tech/rss"},
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


def load_local_env_file(env_path: Path | None = None) -> None:
    path = env_path or BASE_DIR / ".env"
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for line in lines:
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#") or "=" not in cleaned:
            continue
        if cleaned.startswith("export "):
            cleaned = cleaned[len("export ") :].strip()
        key, value = cleaned.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


load_local_env_file()


st.set_page_config(page_title="Rafik Moulouel", layout="wide")


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
    except sqlite3.Error:
        pass


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
    return frame[["ticker", "name", "exchange", "asset_type"]]


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
    return frame[["ticker", "name", "exchange", "asset_type"]]


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
    cryptos = pd.read_csv(CRYPTO_CACHE_PATH)
    cryptos["cik"] = ""

    indices = pd.DataFrame(MAJOR_INDICES)
    indices["cik"] = ""

    catalog = pd.concat([companies, indices, cryptos], ignore_index=True, sort=False)
    catalog["ticker"] = catalog["ticker"].astype(str).str.strip().str.upper()
    catalog["name"] = catalog["name"].astype(str).str.strip()
    catalog["exchange"] = catalog["exchange"].fillna("")
    catalog["asset_type"] = catalog["asset_type"].fillna("Entreprise")
    catalog["label"] = catalog.apply(
        lambda row: f"{row['name']} ({row['ticker']}) - {row['exchange'] or row['asset_type']}",
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
        if not series.empty:
            series_map[ticker] = series

    history = pd.DataFrame(series_map).sort_index()
    if history.empty:
        raise ValueError("Impossible de construire un historique de prix exploitable.")
    return history


@st.cache_data(ttl=900, show_spinner=False)
def download_price_histories(tickers: tuple[str, ...], period: str, interval: str) -> tuple[pd.DataFrame, pd.DataFrame]:
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


@st.cache_data(ttl=900, show_spinner=False)
def fetch_midcap_recommendations(limit: int = 8) -> pd.DataFrame:
    if yf is None:
        return pd.DataFrame()

    query = yf.EquityQuery(
        "and",
        [
            yf.EquityQuery("eq", ["region", "us"]),
            yf.EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
            yf.EquityQuery("gte", ["intradaymarketcap", 2_000_000_000]),
            yf.EquityQuery("lte", ["intradaymarketcap", 10_000_000_000]),
            yf.EquityQuery("gte", ["intradayprice", 5]),
            yf.EquityQuery("gt", ["dayvolume", 250_000]),
        ],
    )

    try:
        payload = yf.screen(query, size=100, sortField="percentchange", sortAsc=False)
    except Exception:
        return pd.DataFrame()

    rows = []
    excluded_name_pattern = re.compile(
        r"Warrant|Rights?|Units?|Preferred|Depositary|Trust Preferred|Acquisition Corp",
        re.IGNORECASE,
    )
    for quote in payload.get("quotes", []):
        ticker = str(quote.get("symbol") or "").upper()
        name = str(quote.get("shortName") or quote.get("longName") or ticker)
        if not ticker or excluded_name_pattern.search(name):
            continue

        price = quote.get("regularMarketPrice") or quote.get("intradayprice")
        day_change = quote.get("regularMarketChangePercent") or quote.get("percentchange")
        market_cap = quote.get("marketCap") or quote.get("intradaymarketcap")
        volume = quote.get("regularMarketVolume") or quote.get("dayvolume")
        fifty_day = quote.get("fiftyDayAverage")
        two_hundred_day = quote.get("twoHundredDayAverage")
        week_high = quote.get("fiftyTwoWeekHigh")
        week_low = quote.get("fiftyTwoWeekLow")

        numeric_values = pd.to_numeric(
            pd.Series(
                {
                    "price": price,
                    "day_change": day_change,
                    "market_cap": market_cap,
                    "volume": volume,
                    "fifty_day": fifty_day,
                    "two_hundred_day": two_hundred_day,
                    "week_high": week_high,
                    "week_low": week_low,
                }
            ),
            errors="coerce",
        )
        price = numeric_values["price"]
        day_change = numeric_values["day_change"]
        market_cap = numeric_values["market_cap"]
        volume = numeric_values["volume"]
        fifty_day = numeric_values["fifty_day"]
        two_hundred_day = numeric_values["two_hundred_day"]
        week_high = numeric_values["week_high"]
        week_low = numeric_values["week_low"]
        if pd.isna(price) or pd.isna(market_cap) or pd.isna(day_change):
            continue

        reasons = []
        score = 0.0
        if 2_000_000_000 <= market_cap <= 10_000_000_000:
            score += 1.5
            reasons.append("taille mid-cap")
        if not pd.isna(volume) and volume >= 1_000_000:
            score += 1.2
            reasons.append("bonne liquidite")
        if not pd.isna(day_change) and day_change > 0:
            score += min(float(day_change), 8.0) * 0.35
            reasons.append("momentum seance positif")
        if not pd.isna(fifty_day) and price > fifty_day:
            score += 1.4
            reasons.append("cours au-dessus de la moyenne 50j")
        if not pd.isna(fifty_day) and not pd.isna(two_hundred_day) and fifty_day > two_hundred_day:
            score += 1.8
            reasons.append("tendance 50j > 200j")
        if not pd.isna(week_high) and week_high > 0 and price >= week_high * 0.85:
            score += 1.0
            reasons.append("proche des plus hauts 52 semaines")
        if not pd.isna(week_low) and week_low > 0 and price >= week_low * 1.25:
            score += 0.8
            reasons.append("rebond confirme vs point bas")

        rows.append(
            {
                "Nom": name,
                "Ticker": ticker,
                "Marche": quote.get("fullExchangeName") or "-",
                "Cours": round(float(price), 2),
                "Variation seance (%)": round(float(day_change), 2),
                "Capitalisation": format_money(market_cap),
                "Volume": format_large_number(volume),
                "Score": round(min(score, 10.0), 1),
                "Pourquoi": ", ".join(reasons[:3]) or "profil a verifier",
            }
        )

    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    return frame.sort_values(["Score", "Variation seance (%)"], ascending=[False, False]).head(limit).reset_index(drop=True)


def render_midcap_recommendations_section() -> None:
    st.subheader("Mid-cap a fort potentiel")
    st.caption(
        "Selection quantitative de valeurs US entre 2B$ et 10B$ de capitalisation, a considerer comme une liste d'idees a analyser."
    )

    with st.spinner("Je cherche des mid-cap avec momentum et liquidite..."):
        try:
            recommendations = fetch_midcap_recommendations(limit=8)
        except Exception as exc:  # pragma: no cover - depends on network/provider
            st.info(f"Impossible de charger les idees mid-cap pour le moment : {exc}")
            return

    if recommendations.empty:
        st.info("Aucune mid-cap exploitable pour le moment.")
        return

    st.dataframe(
        recommendations,
        width="stretch",
        hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score",
                min_value=0,
                max_value=10,
                format="%.1f",
            )
        },
    )

    with st.expander("Methode de selection"):
        st.write(
            "Le score favorise les actions mid-cap liquides, en hausse sur la seance, au-dessus de leur moyenne 50 jours, "
            "avec une tendance 50 jours superieure a la moyenne 200 jours quand ces donnees sont disponibles. "
            "Ce n'est pas un conseil financier : verifie les fondamentaux, la valorisation, les resultats et le risque avant toute decision."
        )


def render_market_movers_section(catalog: pd.DataFrame) -> None:
    st.subheader("A la une")
    st.caption("Un coup d'oeil rapide sur les grandes capitalisations des gros indices US, pas sur les petites valeurs speculatives.")

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
                border-radius:18px;
                padding:18px 20px;
                box-shadow:0 10px 24px rgba(15, 23, 42, 0.08);
                min-height:420px;
            ">
                <div style="font-size:0.88rem;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;color:{accent};">
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
        <style>
        div[data-testid="stVerticalBlock"] > div:has(> .news-card) {
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 12px;
            background: #ffffff;
        }
        .news-card-title {
            font-size: 1.02rem;
            line-height: 1.28;
            font-weight: 700;
            margin: 0 0 6px;
        }
        .news-card-meta {
            color: #64748b;
            font-size: 0.8rem;
            margin-bottom: 8px;
        }
        .news-card-summary {
            color: #334155;
            font-size: 0.92rem;
            line-height: 1.45;
            margin: 0;
        }
        </style>
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
        text_lines.extend(["", "Idees mid-cap"])
        html_sections.append("<h2>Idees mid-cap</h2><ul>")
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


def compact_news_item(item: dict) -> dict:
    return {
        "title": clean_summary_text(item.get("title") or "", max_length=160),
        "summary": clean_summary_text(item.get("summary") or "", max_length=220),
        "source": item.get("source") or item.get("provider") or item.get("feed_label") or "Source inconnue",
        "feed": item.get("feed_label") or item.get("ticker") or "",
        "published_at": item.get("published_at") or "",
        "url": item.get("url") or "",
    }


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
}


def title_keywords(value: str) -> set[str]:
    normalized = normalize_news_title(value)
    return {
        word
        for word in normalized.split()
        if len(word) >= 4 and word not in FRENCH_STOPWORDS and not word.isdigit()
    }


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
        else:
            clusters.append(
                {
                    "keywords": set(keywords),
                    "items": [item],
                    "sources": {item.get("source") or "Source inconnue"},
                }
            )

    ranked = sorted(
        clusters,
        key=lambda cluster: (len(cluster["items"]), len(cluster["sources"]), len(cluster["keywords"])),
        reverse=True,
    )
    result = []
    for cluster in ranked[:limit]:
        first_item = cluster["items"][0]
        result.append(
            {
                "main_title": first_item.get("title") or "",
                "sources": sorted(cluster["sources"]),
                "source_count": len(cluster["sources"]),
                "article_count": len(cluster["items"]),
                "keywords": sorted(cluster["keywords"])[:10],
                "related_titles": [item.get("title") for item in cluster["items"][:5] if item.get("title")],
            }
        )
    return result


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
) -> dict:
    selected_categories = [category for category in categories if category in GENERAL_NEWS_FEEDS] or ["A la une"]
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    category_sections = []
    all_news: list[dict] = []
    for category in selected_categories:
        items = sort_general_news_items(fetch_general_news(category), "Plus recentes")[:items_per_category]
        compact_items = [compact_news_item(item) for item in items]
        category_sections.append({"category": category, "items": compact_items})
        all_news.extend(compact_items)

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

    return {
        "generated_at": generated_at,
        "categories": selected_categories,
        "top_news": dedupe_compact_news(all_news, limit=35),
        "topic_clusters": build_news_topic_clusters(all_news, limit=8),
        "category_sections": category_sections,
        "market_snapshot": market_snapshot_records() if include_market_context else [],
        "gainers": dataframe_records(gainers, limit=5),
        "losers": dataframe_records(losers, limit=5),
        "midcaps": dataframe_records(midcaps, limit=5),
        "portfolio": portfolio_briefing_summary(current_user) if include_portfolio else {},
        "editorial_note": (
            "Briefing generaliste : les marches financiers doivent etre traites comme un sujet parmi les autres, "
            "uniquement s'ils ressortent des rubriques selectionnees ou si le contexte marche est explicitement inclus."
        ),
        "source_labels": sorted({item["source"] for item in all_news if item.get("source")}),
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
    target_words = {3: "450 a 600", 5: "750 a 950", 10: "1300 a 1600"}.get(duration_minutes, "1300 a 1600")
    context_json = json.dumps(context, ensure_ascii=False, default=str)[:22000]
    return f"""
Tu es redacteur en chef d'un podcast quotidien en francais.

Objectif : produire un script parle naturel d'environ {duration_minutes} minutes, soit {target_words} mots.
Ton : {tone}.

Structure souhaitee :
- Commence simplement par "Bonjour Rafik." puis va directement au vif du sujet.
- Pas d'introduction lourde, pas de phrase du type "ce briefing est genere par IA", pas de presentation technique.
- Identifie le sujet principal du jour a partir de topic_clusters et top_news.
- Creuse ce sujet principal un peu plus que les autres : contexte, pourquoi il ressort, angles differents entre sources, ce qu'il faut surveiller.
- Ensuite seulement, fais un tour des autres infos importantes, de maniere concise.
- Termine courtement, sans conclusion pompeuse.

Contraintes :
- Ne recopie pas longuement les articles. Resume et reformule.
- N'invente aucun fait absent du contexte.
- Cite legerement les medias quand c'est utile, sans transformer le podcast en bibliographie.
- Ce briefing est generaliste. Ne fais pas de focus particulier sur les marches financiers.
- Traite les marches financiers comme n'importe quel autre sujet d'actualite : seulement s'ils sont importants dans les infos selectionnees ou si le contexte marche est fourni explicitement.
- Ne cree pas de rubrique financiere obligatoire.
- Structure le briefing : sujet principal approfondi, autres infos, portefeuille uniquement si disponible, points a surveiller.
- Ajoute un avertissement financier uniquement si tu parles explicitement de portefeuille, d'actions ou d'investissement.
- Ecris uniquement le script final, sans JSON ni commentaires techniques.

Contexte structure :
{context_json}
""".strip()


def extract_openai_output_text(payload: dict) -> str:
    if payload.get("output_text"):
        return str(payload["output_text"]).strip()
    pieces: list[str] = []
    for output_item in payload.get("output", []) or []:
        for content in output_item.get("content", []) or []:
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                pieces.append(str(content["text"]))
    return "\n".join(pieces).strip()


def generate_podcast_script(context: dict, duration_minutes: int, tone: str) -> tuple[str, str]:
    api_key = get_openai_api_key()
    if not api_key:
        return build_fallback_podcast_script(context, duration_minutes), "fallback"

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
            "max_output_tokens": 2600,
        },
        timeout=90,
    )
    response.raise_for_status()
    script = extract_openai_output_text(response.json())
    if not script:
        raise ValueError("Le modele n'a pas renvoye de script exploitable.")
    return script, "openai"


def split_tts_script(script: str, max_chars: int = 3800) -> list[str]:
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
    input_text = script.strip()
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
) -> go.Figure:
    fig = go.Figure()
    x_values = format_chart_index(history.index) if compress_time_axis else history.index
    for ticker in history.columns:
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=history[ticker],
                mode="lines",
                connectgaps=True,
                name=label_by_ticker.get(ticker, ticker),
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
) -> go.Figure:
    fig = go.Figure()
    x_values = format_chart_index(performance.index) if compress_time_axis else performance.index
    for ticker in performance.columns:
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=performance[ticker],
                mode="lines",
                connectgaps=True,
                name=label_by_ticker.get(ticker, ticker),
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
    price_history, _ = download_price_histories(tickers, period="5d", interval="1d")
    prices: dict[str, float] = {}
    for ticker in tickers:
        if ticker not in price_history.columns:
            continue
        series = price_history[ticker].dropna()
        if not series.empty:
            prices[ticker] = float(series.iloc[-1])
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
            auto_adjust=False,
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
    period = choose_history_period_from_date(earliest_date)
    tickers = tuple(sorted(priced["Ticker"].unique()))
    try:
        price_history, _ = download_price_histories(tickers, period=period, interval="1d")
    except Exception:
        return pd.DataFrame()

    start_date = pd.to_datetime(earliest_date, errors="coerce")
    if pd.isna(start_date):
        return pd.DataFrame()
    if isinstance(price_history.index, pd.DatetimeIndex):
        price_history = price_history[price_history.index.normalize() >= start_date.normalize()]
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
        active_mask = price_history.index.normalize() >= purchase_date.normalize()
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
            y=history["Performance (%)"],
            mode="lines",
            name="Portefeuille",
            line=dict(color="#2563eb", width=3),
            hovertemplate="Date : %{x}<br>Performance : %{y:.2f}%<extra></extra>",
        )
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.update_layout(
        title="Performance du portefeuille",
        xaxis_title="Date",
        yaxis_title="Performance (%)",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=30, r=30, t=60, b=30),
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
        rows.append(
            {
                "Nom": row["name"],
                "Ticker": ticker,
                "Type": row["asset_type"],
                "Marche": row["exchange"] or "-",
                "Debut periode": round(start_price, 2),
                "Dernier cours": round(end_price, 2),
                "Variation (%)": round(performance, 2),
            }
        )

    return pd.DataFrame(rows).sort_values("Variation (%)", ascending=False).reset_index(drop=True)


def render_header(catalog: pd.DataFrame, cache_path: Path) -> None:
    st.title("Comparateur Boursier Interactif")
    st.caption(
        "Recherche par nom d'entreprise ou d'indice, comparaison multi-actifs, evolution des prix et performances."
    )

    last_refresh = time.strftime("%d/%m/%Y %H:%M", time.localtime(cache_path.stat().st_mtime))
    col1, col2, col3 = st.columns([1, 1, 2])
    col1.metric("Actifs repertories", f"{len(catalog):,}".replace(",", " "))
    col2.metric("Derniere mise a jour annuaire", last_refresh)
    col3.caption(
        "Sources : Nasdaq Trader pour les societes cotees US, CoinGecko pour les cryptos, plus une petite liste d'indices majeurs."
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
    st.subheader("Portefeuille simule")
    st.caption("Dis simplement combien tu veux investir dans un actif. L'app calcule la quantite et affiche la performance.")

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

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Capital investi", format_money(total_cost))
    metric_col2.metric("Valeur actuelle", format_money(total_value))
    metric_col3.metric("PnL latent", format_money(total_pnl), delta=format_percent(total_return))
    metric_col4.metric("Lignes suivies", len(portfolio), delta=f"{missing_quotes} sans cotation" if missing_quotes else None)
    if missing_quotes:
        st.caption("Les positions sans cotation recente restent visibles, mais elles sont exclues du PnL et de la performance totale.")

    portfolio_history = build_portfolio_performance_history(portfolio)
    if not portfolio_history.empty:
        st.plotly_chart(build_portfolio_performance_figure(portfolio_history), width="stretch")
    else:
        st.info("La courbe de performance apparaitra des que l'historique de prix sera disponible.")

    display_portfolio = portfolio[
        [
            "Nom",
            "Ticker",
            "Type",
            "Secteur",
            "Devise",
            "Quantite",
            "Prix achat",
            "Dernier cours",
            "PRU total",
            "Valeur actuelle",
            "PnL latent",
            "PnL latent (%)",
            "Contribution perf (%)",
            "Date achat",
            "Note",
        ]
    ].copy()
    display_portfolio = display_portfolio.rename(
        columns={
            "Quantite": "Quantite calculee",
            "Prix achat": "Prix utilise",
            "PRU total": "Montant investi",
        }
    )
    st.dataframe(
        display_portfolio,
        width="stretch",
        hide_index=True,
        column_config={
            "PnL latent (%)": st.column_config.NumberColumn("PnL latent (%)", format="%.2f%%"),
            "Contribution perf (%)": st.column_config.NumberColumn("Contribution perf (%)", format="%.2f%%"),
            "Quantite calculee": st.column_config.NumberColumn("Quantite calculee", format="%.6g"),
            "Prix utilise": st.column_config.NumberColumn("Prix utilise", format="%.2f"),
            "Dernier cours": st.column_config.NumberColumn("Dernier cours", format="%.2f"),
            "Montant investi": st.column_config.NumberColumn("Montant investi", format="%.2f"),
            "Valeur actuelle": st.column_config.NumberColumn("Valeur actuelle", format="%.2f"),
            "PnL latent": st.column_config.NumberColumn("PnL latent", format="%.2f"),
        },
    )

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
    st.subheader("Marche du jour")
    st.caption("Indices majeurs, stress de marche, taux, matieres premieres, crypto et dernieres nouvelles.")

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
        other_rows = snapshot[snapshot["Groupe"] != "Indices"].copy()

        metric_cols = st.columns(min(4, max(1, len(index_rows))))
        for col, (_, row) in zip(metric_cols, index_rows.iterrows()):
            col.metric(row["Actif"], row["Dernier"], delta=format_percent(row["1j"]))

        st.dataframe(
            snapshot,
            width="stretch",
            hide_index=True,
            column_config={
                "Dernier": st.column_config.NumberColumn("Dernier", format="%.2f"),
                "1j": st.column_config.NumberColumn("1j", format="%.2f%%"),
                "1m": st.column_config.NumberColumn("1m", format="%.2f%%"),
            },
        )

        if not other_rows.empty:
            fig = go.Figure(
                go.Bar(
                    x=other_rows["Actif"],
                    y=other_rows["1m"].fillna(0),
                    marker_color=["#15803d" if value >= 0 else "#b91c1c" for value in other_rows["1m"].fillna(0)],
                    hovertemplate="%{x}<br>Variation 1m : %{y:.2f}%<extra></extra>",
                )
            )
            fig.update_layout(
                title="Performance 1 mois des actifs de contexte",
                template="plotly_white",
                xaxis_title="Actif",
                yaxis_title="Variation 1 mois (%)",
                margin=dict(l=30, r=30, t=60, b=30),
            )
            st.plotly_chart(fig, width="stretch")

    mover_col, news_col = st.columns([1, 1])
    with mover_col:
        render_market_movers_section(catalog)

    with news_col:
        st.subheader("News cles")
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
                if url:
                    st.markdown(f"**[{title}]({url})**")
                else:
                    st.markdown(f"**{title}**")
                st.caption(f"{item.get('ticker')} | {provider} | {published}")


def render_news_email_controls(current_user: sqlite3.Row) -> None:
    if current_user["role"] != "admin":
        return

    with st.expander("Envoyer un recap par email"):
        default_recipients = os.getenv("NEWS_EMAIL_TO") or os.getenv("NEWS_EMAIL_RECIPIENTS") or ""
        base_recipients = st.text_area(
            "Destinataires par defaut",
            value=default_recipients,
            placeholder="email1@example.com, email2@example.com",
            height=80,
        )
        extra_recipients = st.text_area(
            "Ajouter d'autres destinataires pour cet envoi",
            value="",
            placeholder="autre@example.com, equipe@example.com",
            height=80,
        )
        recipients = merge_email_recipient_values(base_recipients, extra_recipients)
        recipient_count = len(split_email_recipients(recipients))
        st.caption(f"Ce recap sera envoye a {recipient_count} destinataire(s).")

        selected_categories = st.multiselect(
            "Rubriques",
            options=list(GENERAL_NEWS_FEEDS.keys()),
            default=list(NEWS_RECAP_DEFAULT_CATEGORIES),
        )
        items_per_category = st.slider("Articles par rubrique", min_value=3, max_value=10, value=NEWS_RECAP_DEFAULT_LIMIT)
        smtp_user = st.text_input(
            "Compte SMTP",
            value=os.getenv("NEWS_SMTP_USER") or os.getenv("GMAIL_USER") or "",
            placeholder="adresse@gmail.com",
        )
        smtp_password = st.text_input(
            "Mot de passe d'application SMTP",
            value="",
            type="password",
            placeholder="Utilise .env pour ne pas le ressaisir",
        )

        config_preview = get_news_email_config(
            recipients,
            sender_override=smtp_user,
            username_override=smtp_user,
            password_override=smtp_password,
        )
        config_errors = validate_news_email_config(config_preview)
        if config_errors:
            st.warning("Configuration mail incomplete : " + " ".join(config_errors))

        if st.button("Envoyer le recap maintenant", disabled=bool(config_errors), use_container_width=True):
            with st.spinner("Envoi du recap en cours..."):
                try:
                    result = send_daily_news_recap_email(
                        recipients_override=recipients,
                        categories=selected_categories,
                        items_per_category=items_per_category,
                        sender_override=smtp_user,
                        username_override=smtp_user,
                        password_override=smtp_password,
                    )
                except Exception as exc:
                    st.error(f"Impossible d'envoyer le recap : {exc}")
                else:
                    st.success(f"Recap envoye a {result['sent_count']} destinataire(s).")


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
        include_portfolio = st.toggle("Inclure le portefeuille simule", value=True, key="podcast_include_portfolio")
        include_market_context = st.toggle(
            "Inclure un contexte marche dedie",
            value=False,
            key="podcast_include_market_context",
            help="Desactive par defaut : les marches restent un sujet comme les autres dans le briefing generaliste.",
        )

        if st.button("Generer le script du podcast", use_container_width=True):
            with st.spinner("Je collecte les infos, dedoublonne les sujets et prepare le script..."):
                try:
                    context = collect_podcast_briefing_context(
                        current_user,
                        selected_categories,
                        include_portfolio=include_portfolio,
                        include_market_context=include_market_context,
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
                    value="Voix francaise naturelle, rythme pose, ton briefing radio professionnel.",
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


def render_news_section(catalog: pd.DataFrame, comparison_tickers: list[str], current_user: sqlite3.Row) -> None:
    general_tab, market_tab = st.tabs(["Infos generales", "News marche"])

    with market_tab:
        st.subheader("Actualites marche")
        st.caption("Flux recents par actif depuis Yahoo Finance. Utilise le bouton de rafraichissement pour recharger les news.")

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

                    st.markdown(f"### {title}")
                    st.caption(f"{name} ({ticker}) | Source : {provider} | {published}")
                    if summary:
                        st.write(summary)
                    if url:
                        st.markdown(f"[Ouvrir l'article]({url})")
                    st.divider()

    with general_tab:
        st.subheader("Infos generales")
        st.caption("Actualites generalistes issues de plusieurs medias : presse nationale, chaines info et medias internationaux francophones.")

        category = st.selectbox(
            "Rubrique",
            options=list(GENERAL_NEWS_FEEDS.keys()),
            key="general_news_category",
        )
        if st.button("Rafraichir les infos generales", key="refresh_general_news_button"):
            fetch_general_news.clear()

        render_news_email_controls(current_user)
        render_podcast_briefing_controls(current_user)

        with st.expander("Sources et methode"):
            st.write(
                "Je lis directement des flux RSS publics de medias d'information, puis j'affiche les articles les plus recents. "
                "Chaque carte indique la source du media et le lien direct de l'article. "
                "Si un flux ne repond pas, il est ignore pour ne pas bloquer la page."
            )
            for feed in GENERAL_NEWS_FEEDS[category]:
                st.markdown(f"- `{feed['label']}` : {feed['url']}")

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
    selected_period_label: str,
    smooth_closures: bool,
    use_log_scale: bool,
) -> list[str]:
    st.subheader("Comparateur")
    st.caption("Compare plusieurs actions, indices, ETF ou cryptos avec les courbes de performance et de prix.")

    visible_catalog = catalog[catalog["asset_type"].isin(asset_types)].copy()
    if visible_catalog.empty:
        st.warning("Aucun actif ne correspond au filtre choisi.")
        return []

    default_labels = visible_catalog[visible_catalog["ticker"].isin(DEFAULT_TICKERS)]["label"].tolist()
    selected_labels = st.multiselect(
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

    selected_assets = visible_catalog[visible_catalog["label"].isin(selected_labels)].copy()
    selected_assets = selected_assets.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    comparison_tickers = selected_assets["ticker"].tolist()
    tickers = tuple(comparison_tickers)
    label_by_ticker = dict(zip(selected_assets["ticker"], selected_assets["name"]))

    period_config = PERIOD_OPTIONS[selected_period_label]
    with st.spinner("Je recupere les historiques de marche..."):
        try:
            price_history, return_history = download_price_histories(
                tickers=tickers,
                period=period_config["period"],
                interval=period_config["interval"],
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

    display_history = build_display_history(price_history, smooth_closures)
    display_return_history = build_display_history(return_history, smooth_closures)
    performance = compute_performance_frame(display_return_history)

    price_figure = build_price_figure(display_history, label_by_ticker, smooth_closures)
    if use_log_scale:
        price_figure.update_yaxes(type="log")

    summary_table = build_summary_table(selected_assets, price_history, return_history)

    st.subheader("Vue d'ensemble")
    st.dataframe(summary_table, width="stretch", hide_index=True)

    performance_tab, price_tab = st.tabs(["Performance (%)", "Prix"])
    with performance_tab:
        st.plotly_chart(
            build_performance_figure(performance, label_by_ticker, smooth_closures),
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
        + " Les performances (%) sont calculees sur une serie ajustee quand elle est disponible."
    )
    return comparison_tickers


def default_comparison_tickers(catalog: pd.DataFrame, asset_types: list[str]) -> list[str]:
    visible_catalog = catalog[catalog["asset_type"].isin(asset_types)].copy()
    if visible_catalog.empty:
        return []
    defaults = visible_catalog[visible_catalog["ticker"].isin(DEFAULT_TICKERS)]
    return defaults.drop_duplicates(subset=["ticker"])["ticker"].tolist()


def main() -> None:
    current_user = require_authenticated_user()
    render_auth_cookie_sync()
    if current_user is None:
        return

    ready, reason = market_data_is_ready()
    if not ready:
        st.error(reason)
        return

    with st.sidebar:
        render_account_sidebar(current_user)
        st.divider()
        st.header("Parametres")
        refresh_catalog = st.button("Rafraichir actions / indices")
        refresh_crypto = st.button("Rafraichir cryptos")
        asset_types = st.multiselect(
            "Types d'actifs visibles",
            options=["Entreprise", "Indice", "ETF", "Crypto"],
            default=["Entreprise", "Indice", "Crypto"],
        )
        selected_period_label = st.selectbox(
            "Periode d'observation",
            options=list(PERIOD_OPTIONS.keys()),
            index=4,
        )
        smooth_closures = st.toggle(
            "Lisser les fermetures de marche",
            value=True,
            help="Prolonge visuellement la derniere cotation connue pendant les fermetures pour eviter les cassures de courbe.",
        )
        use_log_scale = st.toggle("Echelle logarithmique sur le graphe prix", value=False)

    try:
        cache_path = download_company_directory(force_refresh=refresh_catalog)
        crypto_cache_path = download_crypto_directory(force_refresh=refresh_crypto)
    except Exception as exc:  # pragma: no cover - depends on network/provider
        st.error(f"Impossible de telecharger l'annuaire des tickers : {exc}")
        return

    catalog = load_symbol_catalog(max(cache_path.stat().st_mtime, crypto_cache_path.stat().st_mtime))
    render_header(catalog, cache_path)
    page_names = ["Comparateur", "Portefeuille", "Marche du jour", "Analyse", "Actualites"]
    if current_user["role"] == "admin":
        page_names.append("Admin utilisateurs")
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
            selected_period_label,
            smooth_closures,
            use_log_scale,
        )
        st.session_state["comparison_tickers"] = comparison_tickers

    elif selected_page == "Marche du jour":
        render_market_today_section(catalog)

    elif selected_page == "Portefeuille":
        render_portfolio_section(catalog, current_user)

    elif selected_page == "Analyse":
        render_market_movers_section(catalog)
        st.divider()
        render_midcap_recommendations_section()
        st.divider()
        render_company_profile_section(catalog)

    elif selected_page == "Actualites":
        render_news_section(catalog, comparison_tickers, current_user)

    elif selected_page == "Admin utilisateurs" and current_user["role"] == "admin":
        render_user_management_section(current_user)


def run_daily_news_email_command() -> int:
    try:
        result = send_daily_news_recap_email()
    except Exception as exc:
        print(f"Erreur envoi recap infos : {exc}", file=sys.stderr)
        return 1
    print(f"Recap infos envoye a {result['sent_count']} destinataire(s).")
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "send-daily-news-email":
        raise SystemExit(run_daily_news_email_command())
    main()
