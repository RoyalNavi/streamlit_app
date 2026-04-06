import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import StringIO
from pathlib import Path
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
MARKET_CACHE_PATH = DATA_DIR / "market_directory.csv"
CRYPTO_CACHE_PATH = DATA_DIR / "crypto_directory.csv"
WATCHLIST_PATH = DATA_DIR / "watchlist.json"
MARKET_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
CRYPTO_CACHE_TTL_SECONDS = 12 * 60 * 60
USER_AGENT = "rafik-streamlit-app/1.0 (finance dashboard)"
MAX_COMPARISON_COUNT = 8
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"

MAJOR_INDICES = [
    {"ticker": "^GSPC", "name": "S&P 500", "exchange": "Index", "asset_type": "Indice"},
    {"ticker": "^IXIC", "name": "Nasdaq Composite", "exchange": "Index", "asset_type": "Indice"},
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

DEFAULT_TICKERS = ["AAPL", "MSFT", "^FCHI", "BTC-USD"]
OTHER_EXCHANGE_NAMES = {
    "A": "NYSE American",
    "N": "NYSE",
    "P": "NYSE Arca",
    "Z": "Cboe BZX",
    "V": "IEX",
}
DEFAULT_WATCHLIST_TICKERS = ["AAPL", "MSFT", "^FCHI", "BTC-USD", "ETH-USD"]
GENERAL_NEWS_FEEDS = {
    "A la une": [
        {"label": "Franceinfo - Les titres", "url": "https://www.francetvinfo.fr/titres.rss"},
        {"label": "Le Monde - A la une", "url": "https://www.lemonde.fr/rss/une.xml"},
        {"label": "Le Figaro - Actualites", "url": "https://www.lefigaro.fr/rss/figaro_actualites.xml"},
        {"label": "20 Minutes - A la une", "url": "https://www.20minutes.fr/feeds/rss-une.xml"},
    ],
    "Politique": [
        {"label": "Le Monde - Politique", "url": "https://www.lemonde.fr/politique/rss_full.xml"},
        {"label": "Le Figaro - Politique", "url": "https://www.lefigaro.fr/rss/figaro_politique.xml"},
        {"label": "France 24 - France", "url": "https://www.france24.com/fr/france/rss"},
    ],
    "International / Infos": [
        {"label": "France 24 - Fil general", "url": "https://www.france24.com/fr/rss"},
        {"label": "Le Monde - A la une", "url": "https://www.lemonde.fr/rss/une.xml"},
        {"label": "Franceinfo - Les titres", "url": "https://www.francetvinfo.fr/titres.rss"},
    ],
}


st.set_page_config(page_title="Comparateur Boursier", layout="wide")


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


def load_saved_watchlist() -> list[str]:
    ensure_data_dir()
    if not WATCHLIST_PATH.exists():
        return DEFAULT_WATCHLIST_TICKERS.copy()
    try:
        payload = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return DEFAULT_WATCHLIST_TICKERS.copy()

    tickers = payload.get("tickers", [])
    if not isinstance(tickers, list):
        return DEFAULT_WATCHLIST_TICKERS.copy()
    cleaned = [str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()]
    return cleaned or DEFAULT_WATCHLIST_TICKERS.copy()


def save_watchlist(tickers: list[str]) -> None:
    ensure_data_dir()
    payload = {"tickers": tickers, "updated_at": int(time.time())}
    WATCHLIST_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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


@st.cache_data(ttl=900, show_spinner=False)
def download_price_history(tickers: tuple[str, ...], period: str, interval: str) -> pd.DataFrame:
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

    closes: dict[str, pd.Series] = {}
    for ticker in tickers:
        if len(tickers) == 1:
            frame = raw
        else:
            if ticker not in raw.columns.get_level_values(0):
                continue
            frame = raw[ticker]

        if "Close" not in frame:
            continue

        series = frame["Close"].dropna()
        if not series.empty:
            closes[ticker] = series

    history = pd.DataFrame(closes).sort_index()
    if history.empty:
        raise ValueError("Impossible de construire un historique de cloture.")
    return history


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


def build_watchlist_table(catalog: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    rows = []
    catalog_by_ticker = catalog.set_index("ticker")
    for ticker in history.columns:
        series = history[ticker].dropna()
        if series.empty:
            continue
        first_value = float(series.iloc[0])
        last_value = float(series.iloc[-1])
        previous_value = float(series.iloc[-2]) if len(series) > 1 else last_value
        day_change = ((last_value / previous_value) - 1) * 100 if previous_value else 0.0
        period_change = ((last_value / first_value) - 1) * 100 if first_value else 0.0
        row = catalog_by_ticker.loc[ticker]
        rows.append(
            {
                "Nom": row["name"],
                "Ticker": ticker,
                "Type": row["asset_type"],
                "Marche": row["exchange"] or "-",
                "Dernier cours": round(last_value, 2),
                "Variation seance (%)": round(day_change, 2),
                "Variation periode (%)": round(period_change, 2),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["Nom", "Ticker", "Type", "Marche", "Dernier cours", "Variation seance (%)", "Variation periode (%)"])
    return pd.DataFrame(rows).sort_values("Variation periode (%)", ascending=False).reset_index(drop=True)


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

        for item in root.findall("./channel/item")[:10]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            source = (item.findtext("source") or feed["label"]).strip()
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


def build_summary_table(
    catalog: pd.DataFrame,
    history: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    catalog_by_ticker = catalog.set_index("ticker")
    for ticker in history.columns:
        series = history[ticker].dropna()
        if series.empty:
            continue

        start_price = float(series.iloc[0])
        end_price = float(series.iloc[-1])
        performance = ((end_price / start_price) - 1) * 100 if start_price else 0.0
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


def render_watchlist_section(catalog: pd.DataFrame, selected_period_label: str) -> list[str]:
    st.subheader("Ma watchlist")
    saved_tickers = load_saved_watchlist()
    available_watchlist = catalog[catalog["ticker"].isin(saved_tickers)]
    default_labels = available_watchlist["label"].tolist()

    watchlist_labels = st.multiselect(
        "Actifs a surveiller",
        options=catalog["label"].tolist(),
        default=default_labels,
        max_selections=20,
        key="watchlist_labels",
        placeholder="Ajoute des actions, indices ou cryptos a suivre dans le temps.",
    )

    watchlist_assets = catalog[catalog["label"].isin(watchlist_labels)].drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    watchlist_tickers = watchlist_assets["ticker"].tolist()

    save_col, load_col = st.columns(2)
    if save_col.button("Sauvegarder la watchlist", key="save_watchlist_button"):
        save_watchlist(watchlist_tickers)
        st.success("Watchlist sauvegardee.")
    if load_col.button("Recharger la watchlist sauvegardee", key="reload_watchlist_button"):
        st.rerun()

    if not watchlist_tickers:
        st.info("Ajoute quelques actifs a ta watchlist pour surveiller leurs cours.")
        return []

    period_config = PERIOD_OPTIONS[selected_period_label]
    with st.spinner("Je charge les cours de la watchlist..."):
        try:
            watchlist_history = download_price_history(
                tickers=tuple(watchlist_tickers),
                period=period_config["period"],
                interval=period_config["interval"],
            )
        except Exception as exc:  # pragma: no cover - depends on network/provider
            st.error(f"Impossible de charger la watchlist : {exc}")
            return watchlist_tickers

    watchlist_table = build_watchlist_table(catalog, watchlist_history)
    st.dataframe(watchlist_table, width="stretch", hide_index=True)
    return watchlist_tickers


def render_news_section(catalog: pd.DataFrame, watchlist_tickers: list[str], comparison_tickers: list[str]) -> None:
    market_tab, general_tab = st.tabs(["News marche", "Infos generales"])

    with market_tab:
        st.subheader("Actualites marche")
        st.caption("Flux recents par actif depuis Yahoo Finance. Utilise le bouton de rafraichissement pour recharger les news.")

        news_scope = st.radio(
            "Source des actualites",
            options=["Watchlist", "Comparateur"],
            horizontal=True,
            key="news_scope",
        )
        if st.button("Rafraichir les actualites marche", key="refresh_news_button"):
            fetch_news_for_tickers.clear()

        source_tickers = watchlist_tickers if news_scope == "Watchlist" else comparison_tickers
        available_tickers = [ticker for ticker in source_tickers if ticker]
        if not available_tickers:
            st.info("Ajoute des actifs a la watchlist ou au comparateur pour afficher leurs actualites.")
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
        st.caption("Actualites plus generalistes : politique, infos generales, international et sujets de societe.")

        category = st.selectbox(
            "Rubrique",
            options=list(GENERAL_NEWS_FEEDS.keys()),
            key="general_news_category",
        )
        if st.button("Rafraichir les infos generales", key="refresh_general_news_button"):
            fetch_general_news.clear()

        with st.expander("Sources et methode"):
            st.write(
                "Je lis directement des flux RSS publics de medias d'information, puis j'affiche les articles les plus recents. "
                "Chaque carte indique la source du media et le lien direct de l'article."
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
            "Filtrer par flux",
            options=["Tous"] + [feed["label"] for feed in GENERAL_NEWS_FEEDS[category]],
            key="general_news_source_filter",
        )

        with st.spinner("Je recupere les infos generales..."):
            general_news = fetch_general_news(category)

        if source_filter != "Tous":
            general_news = [item for item in general_news if item["feed_label"] == source_filter]

        general_news = sort_general_news_items(general_news, general_sort)

        if not general_news:
            st.info("Aucune actualite generale n'a pu etre chargee pour cette rubrique.")
            return

        for item in general_news[:25]:
            st.markdown(f"### {item['title']}")
            st.caption(
                f"Source : {item['source']} | Flux : {item['feed_label']} | {format_rss_datetime(item['published_at'])}"
            )
            if item["summary"]:
                st.write(item["summary"])
            st.markdown(f"[Lire l'article]({item['url']})")
            st.caption(f"Flux consulte : {item['feed_url']}")
            st.divider()


def main() -> None:
    ready, reason = market_data_is_ready()
    if not ready:
        st.error(reason)
        return

    with st.sidebar:
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
        compress_time_axis = st.toggle(
            "Masquer les trous de fermeture",
            value=True,
            help="Compacte l'axe du temps pour eviter les cassures visuelles dues aux week-ends et fermetures de marche.",
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
    dashboard_tab, news_tab = st.tabs(["Tableau de bord", "Actualites"])

    with dashboard_tab:
        saved_watchlist_tickers = render_watchlist_section(catalog, selected_period_label)
        st.divider()
        st.subheader("Comparateur")

        visible_catalog = catalog[catalog["asset_type"].isin(asset_types)].copy()
        if visible_catalog.empty:
            st.warning("Aucun actif ne correspond au filtre choisi.")
            return

        default_tickers = saved_watchlist_tickers or DEFAULT_TICKERS
        default_labels = visible_catalog[visible_catalog["ticker"].isin(default_tickers)]["label"].tolist()
        selected_labels = st.multiselect(
            "Recherche et comparaison",
            options=visible_catalog["label"].tolist(),
            default=default_labels,
            max_selections=MAX_COMPARISON_COUNT,
            placeholder="Tape le nom d'une entreprise ou d'un indice, puis selectionne-le.",
            help="Tu peux chercher par nom d'entreprise, ticker ou nom d'indice.",
        )

        if not selected_labels:
            st.info("Selectionne au moins un actif pour afficher les graphes.")
            comparison_tickers: list[str] = []
        else:
            selected_assets = visible_catalog[visible_catalog["label"].isin(selected_labels)].copy()
            selected_assets = selected_assets.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
            comparison_tickers = selected_assets["ticker"].tolist()
            tickers = tuple(comparison_tickers)
            label_by_ticker = dict(zip(selected_assets["ticker"], selected_assets["name"]))

            period_config = PERIOD_OPTIONS[selected_period_label]
            with st.spinner("Je recupere les historiques de marche..."):
                try:
                    history = download_price_history(
                        tickers=tickers,
                        period=period_config["period"],
                        interval=period_config["interval"],
                    )
                except Exception as exc:  # pragma: no cover - depends on network/provider
                    st.error(f"Impossible de recuperer les donnees de marche : {exc}")
                    history = pd.DataFrame()

            if not history.empty:
                missing_tickers = [ticker for ticker in tickers if ticker not in history.columns]
                if missing_tickers:
                    st.warning(f"Aucune donnee exploitable pour : {', '.join(missing_tickers)}")

                if use_log_scale:
                    price_figure = build_price_figure(history, label_by_ticker, compress_time_axis)
                    price_figure.update_yaxes(type="log")
                else:
                    price_figure = build_price_figure(history, label_by_ticker, compress_time_axis)

                performance = compute_performance_frame(history)
                summary_table = build_summary_table(selected_assets, history)

                st.subheader("Vue d'ensemble")
                st.dataframe(summary_table, width="stretch", hide_index=True)

                price_tab, performance_tab = st.tabs(["Prix", "Performance (%)"])
                with price_tab:
                    st.plotly_chart(price_figure, width="stretch")
                with performance_tab:
                    st.plotly_chart(
                        build_performance_figure(performance, label_by_ticker, compress_time_axis),
                        width="stretch",
                    )

                st.caption(
                    f"Periode chargee : {selected_period_label} | Intervalle Yahoo Finance : {period_config['interval']} | "
                    "Les prix sont affiches dans leur devise de cotation d'origine. "
                    + (
                        "Les fermetures de marche sont visuellement compactees."
                        if compress_time_axis
                        else "L'axe du temps suit le calendrier reel."
                    )
                )

    with news_tab:
        render_news_section(catalog, saved_watchlist_tickers, comparison_tickers)


if __name__ == "__main__":
    main()
