import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import escape
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
MARKET_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
CRYPTO_CACHE_TTL_SECONDS = 12 * 60 * 60
USER_AGENT = "rafik-streamlit-app/1.0 (finance dashboard)"
MAX_COMPARISON_COUNT = 10
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


st.set_page_config(page_title="Rafik Moulouel", layout="wide")


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
    st.caption("Recherche une entreprise cotee pour voir son activite, son chiffre d'affaires et un sentiment de marche indicatif.")

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

        info_col1, info_col2 = st.columns(2)
        info_col1.write(f"**Secteur** : {snapshot['sector']}")
        info_col1.write(f"**Activite** : {snapshot['industry']}")
        info_col2.write(f"**Sentiment marche** : {snapshot['sentiment_label']}")
        info_col2.caption(snapshot["sentiment_reason"])

        st.write(snapshot["summary"])

        if snapshot["news_items"]:
            st.caption("Dernieres infos")
            for item in snapshot["news_items"]:
                title = item.get("title") or "Sans titre"
                url = item.get("url")
                if url:
                    st.markdown(f"- [{title}]({url})")
                else:
                    st.markdown(f"- {title}")


def render_news_section(catalog: pd.DataFrame, comparison_tickers: list[str]) -> None:
    market_tab, general_tab = st.tabs(["News marche", "Infos generales"])

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
    dashboard_tab, news_tab = st.tabs(["Tableau de bord", "Actualites"])
    comparison_tickers: list[str] = []

    with dashboard_tab:
        render_market_movers_section(catalog)
        st.divider()
        render_company_profile_section(catalog)
        st.divider()
        st.subheader("Comparateur")

        visible_catalog = catalog[catalog["asset_type"].isin(asset_types)].copy()
        if visible_catalog.empty:
            st.warning("Aucun actif ne correspond au filtre choisi.")
            return

        default_tickers = DEFAULT_TICKERS
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
        else:
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
                    price_history = pd.DataFrame()
                    return_history = pd.DataFrame()

            if not price_history.empty:
                missing_tickers = [ticker for ticker in tickers if ticker not in price_history.columns]
                if missing_tickers:
                    st.warning(f"Aucune donnee exploitable pour : {', '.join(missing_tickers)}")

                display_history = build_display_history(price_history, smooth_closures)
                display_return_history = build_display_history(return_history, smooth_closures)
                performance = compute_performance_frame(display_return_history)

                if use_log_scale:
                    price_figure = build_price_figure(display_history, label_by_ticker, smooth_closures)
                    price_figure.update_yaxes(type="log")
                else:
                    price_figure = build_price_figure(display_history, label_by_ticker, smooth_closures)

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

    with news_tab:
        render_news_section(catalog, comparison_tickers)


if __name__ == "__main__":
    main()
