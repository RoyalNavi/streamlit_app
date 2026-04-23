from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any

import requests
import yfinance as yf

from cache import read_cache, write_cache


log = logging.getLogger("news_context")

LLM_CACHE_KEY = "news_context_llm"
LLM_PROMPT_VERSION = "v2_detail"
YAHOO_NEWS_CACHE_KEY = "yahoo_news_context"
YAHOO_NEWS_TTL_MINUTES = int(os.getenv("YAHOO_NEWS_CONTEXT_TTL_MINUTES", "45"))
SMALLCAP_NEWS_CONTEXT_ENABLED = os.getenv("SMALLCAP_NEWS_CONTEXT_ENABLED", "1").lower() not in {"0", "false", "no"}
MAX_NEWS_ITEMS = 5

EARNINGS_TERMS = re.compile(r"\b(earnings|results|quarter|q[1-4]|eps|revenue|guidance|profit|loss)\b", re.I)
OFFERING_TERMS = re.compile(r"\b(offering|registered direct|atm|warrant|dilution|convertible|shelf)\b", re.I)
RUMOR_TERMS = re.compile(r"\b(rumou?r|speculation|reportedly|unconfirmed|chatter|takeover talk)\b", re.I)
COMPANY_TERMS = re.compile(
    r"\b(contract|deal|partnership|approval|fda|launch|buyback|merger|acquisition|"
    r"offering|debt|secures|wins|upgrade|downgrade|raises|cuts|forecast)\b",
    re.I,
)
SECTOR_TERMS = re.compile(
    r"\b(sector|industry|market|oil|gas|bitcoin|crypto|ai|semiconductor|biotech|"
    r"bank|rates|tariff|china|fed|regulation)\b",
    re.I,
)


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def fetch_yahoo_news(ticker: str, limit: int = MAX_NEWS_ITEMS) -> list[dict]:
    try:
        raw_items = yf.Ticker(str(ticker).upper()).news or []
    except Exception as exc:
        log.warning("fetch_yahoo_news(%s) failed: %s", ticker, exc)
        return []

    items = []
    for item in raw_items[:limit]:
        content = item.get("content") if isinstance(item.get("content"), dict) else {}
        title = content.get("title") or item.get("title") or ""
        if not title:
            continue
        provider = content.get("provider", {}).get("displayName") if isinstance(content.get("provider"), dict) else None
        published = content.get("pubDate") or item.get("providerPublishTime") or item.get("pubDate")
        url = content.get("canonicalUrl", {}).get("url") if isinstance(content.get("canonicalUrl"), dict) else item.get("link")
        items.append(
            {
                "title": str(title).strip(),
                "provider": provider or item.get("publisher") or "",
                "published_at": published,
                "url": url or "",
            }
        )
    return items


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", ""))
    except Exception:
        return None


def _load_yahoo_news_cache() -> dict:
    cached = read_cache(YAHOO_NEWS_CACHE_KEY)
    data = cached.get("data") if cached else None
    return data if isinstance(data, dict) else {}


def _save_yahoo_news_cache(data: dict) -> None:
    write_cache(YAHOO_NEWS_CACHE_KEY, data)


def fetch_yahoo_news_cached(ticker: str, limit: int = MAX_NEWS_ITEMS, ttl_minutes: int = YAHOO_NEWS_TTL_MINUTES) -> list[dict]:
    ticker = str(ticker).upper()
    cache = _load_yahoo_news_cache()
    entry = cache.get(ticker)
    now = datetime.utcnow()
    if isinstance(entry, dict):
        fetched_at = _parse_iso(entry.get("fetched_at"))
        if fetched_at and now - fetched_at <= timedelta(minutes=ttl_minutes):
            items = entry.get("items")
            if isinstance(items, list):
                return items[:limit]
    items = fetch_yahoo_news(ticker, limit=limit)
    cache[ticker] = {"fetched_at": utc_now_iso(), "items": items}
    _save_yahoo_news_cache(cache)
    return items


def news_titles_hash(news_items: list[dict]) -> str:
    titles = [str(item.get("title") or "").strip().lower() for item in news_items if item.get("title")]
    normalized = "\n".join(titles)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def classify_news_context(news_items: list[dict], earnings_flag: str | None = None) -> tuple[str, list[str]]:
    titles = [str(item.get("title") or "") for item in news_items if item.get("title")]
    joined = " | ".join(titles)
    if earnings_flag and earnings_flag != "-":
        return "Earnings proches", titles[:3]
    if not titles:
        return "Sans news claire", []
    if EARNINGS_TERMS.search(joined):
        return "Earnings proches", titles[:3]
    if COMPANY_TERMS.search(joined):
        return "Catalyseur société", titles[:3]
    if SECTOR_TERMS.search(joined):
        return "Catalyseur secteur", titles[:3]
    return "Avec news", titles[:3]


def classify_smallcap_news_context(news_items: list[dict]) -> tuple[str, str, list[str], float]:
    titles = [str(item.get("title") or "") for item in news_items if item.get("title")]
    joined = " | ".join(titles)
    if not titles:
        return "no_clear_news", "Sans news claire", [], -0.25
    if OFFERING_TERMS.search(joined):
        return "offering_risk", "Risque dilution/offering", titles[:3], -0.6
    if EARNINGS_TERMS.search(joined):
        return "earnings", "Earnings", titles[:3], 0.15
    if RUMOR_TERMS.search(joined):
        return "rumor_possible", "Rumeur possible", titles[:3], -0.15
    if COMPANY_TERMS.search(joined) or SECTOR_TERMS.search(joined):
        return "news_confirmed", "News confirmée", titles[:3], 0.35
    return "no_clear_news", "Sans news claire", titles[:3], -0.1


def summarize_smallcap_news_context(label: str, headlines: list[str]) -> str:
    first_title = headlines[0] if headlines else ""
    if label == "offering_risk":
        return "Risque de dilution/offering detecte dans les titres Yahoo."
    if label == "earnings":
        return "Contexte resultats/guidance detecte dans les titres Yahoo."
    if label == "rumor_possible":
        return "Mouvement possiblement lie a une rumeur ou une speculation de marche."
    if label == "news_confirmed":
        if first_title:
            return f"Catalyseur identifiable dans les news Yahoo : {first_title[:140]}"
        return "Catalyseur identifiable dans les news Yahoo."
    if first_title:
        return f"News presentes mais catalyseur peu clair : {first_title[:140]}"
    return "Pas de news Yahoo claire pour expliquer le mouvement."


def enrich_rows_with_news_context(
    rows: list[dict],
    *,
    engine: str,
    limit: int = 15,
    news_limit: int = MAX_NEWS_ITEMS,
) -> dict[str, int]:
    enriched = 0
    for row in rows[:limit]:
        ticker = row.get("Ticker") if engine == "standard" else row.get("ticker")
        if not ticker:
            continue
        if engine == "smallcap" and not SMALLCAP_NEWS_CONTEXT_ENABLED:
            row["smallcap_news_label"] = "disabled"
            row["smallcap_news_adjustment"] = 0.0
            continue
        news_items = fetch_yahoo_news_cached(str(ticker), limit=news_limit)
        earnings_flag = row.get("Earnings") if engine == "standard" else None
        label, headlines = classify_news_context(news_items, earnings_flag=earnings_flag)
        row["context_label"] = label
        row["context_headlines"] = headlines
        row["context_news_hash"] = news_titles_hash(news_items) if news_items else None
        row["context_news_count"] = len(news_items)
        if engine == "smallcap":
            small_label, small_display, small_headlines, adjustment = classify_smallcap_news_context(news_items)
            row["smallcap_news_label"] = small_label
            row["smallcap_news_display"] = small_display
            row["smallcap_news_headlines"] = small_headlines
            row["smallcap_news_summary"] = summarize_smallcap_news_context(small_label, small_headlines)
            row["smallcap_news_adjustment"] = adjustment
        enriched += 1
    return {"enriched": enriched}


def _load_llm_cache() -> dict:
    cached = read_cache(LLM_CACHE_KEY)
    data = cached.get("data") if cached else None
    return data if isinstance(data, dict) else {}


def _save_llm_cache(data: dict) -> None:
    write_cache(LLM_CACHE_KEY, data)


def llm_cache_key(ticker: str, news_items: list[dict]) -> str:
    return f"{LLM_PROMPT_VERSION}:{str(ticker).upper()}:{news_titles_hash(news_items)}"


def summarize_news_with_llm_cached(
    ticker: str,
    news_items: list[dict],
    api_key: str,
    *,
    model: str = "gpt-4o-mini",
) -> dict:
    if not news_items:
        return {"summary": "Aucune news Yahoo exploitable pour ce ticker.", "cached": False, "news_hash": None}
    key = llm_cache_key(ticker, news_items)
    cache = _load_llm_cache()
    if key in cache and cache[key].get("summary"):
        return {**cache[key], "cached": True}

    titles = "\n".join(f"- {item.get('title')}" for item in news_items[:MAX_NEWS_ITEMS])
    prompt = (
        "Tu es analyste financier. A partir des titres Yahoo Finance ci-dessous, "
        "produis un contexte court mais utile en francais, sans inventer de fait absent des titres.\n\n"
        "Format attendu, 4 points maximum :\n"
        "Contexte : 1 phrase sur ce que disent les news.\n"
        "Catalyseur probable : evenement ou theme identifiable, sinon dis que ce n'est pas clair.\n"
        "Lecture du signal : 1 phrase reliant les news au mouvement du titre.\n"
        "Risque : 1 risque principal visible dans les titres ou 'non identifiable dans les titres'.\n\n"
        f"Ticker: {ticker}\nTitres:\n{titles}"
    )
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 260,
        },
        timeout=30,
    )
    response.raise_for_status()
    summary = response.json()["choices"][0]["message"]["content"].strip()
    payload = {
        "ticker": str(ticker).upper(),
        "news_hash": news_titles_hash(news_items),
        "headlines": [item.get("title") for item in news_items[:MAX_NEWS_ITEMS]],
        "summary": summary,
        "generated_at": utc_now_iso(),
    }
    cache[key] = payload
    _save_llm_cache(cache)
    return {**payload, "cached": False}
