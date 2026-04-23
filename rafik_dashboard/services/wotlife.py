import html
import json
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
import streamlit as st


WOTLIFE_PLAYER_URL = "https://en.wot-life.com/na/player/Tonald_Drump_-1004845256/"
WOTLIFE_PLAYERS = {
    "Tonald_Drump_": "https://en.wot-life.com/na/player/Tonald_Drump_-1004845256/",
    "Sasuke_Uchiwa_": "https://en.wot-life.com/na/player/Sasuke_Uchiwa_-1003347351/",
}
WOTLIFE_TIMEOUT_SECONDS = 15


def _clean_html_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace("%", "")
    text = text.replace(".", "").replace(",", ".") if "," in text else text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _extract_stats_history(page_html: str) -> pd.DataFrame:
    match = re.search(r"var\s+stats_history\s*=\s*(\[.*?\]);", page_html, flags=re.S)
    if not match:
        return pd.DataFrame(columns=["updated_at", "date", "wn8", "winrate", "battles"])

    try:
        rows = json.loads(match.group(1))
    except json.JSONDecodeError:
        return pd.DataFrame(columns=["updated_at", "date", "wn8", "winrate", "battles"])

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["updated_at", "date", "wn8", "winrate", "battles"])

    frame = frame.rename(columns={"wr": "winrate"})
    frame["date"] = pd.to_datetime(frame["updated_at"], unit="s", utc=True).dt.tz_convert(None)
    for column in ("wn8", "winrate", "battles"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame[["updated_at", "date", "wn8", "winrate", "battles"]].dropna(subset=["date"])


def _extract_overall_stats(page_html: str) -> dict[str, list[str]]:
    table_match = re.search(r"<h3>Overall Stats</h3>\s*<table[^>]*>(.*?)</table>", page_html, flags=re.S)
    if not table_match:
        return {}

    stats: dict[str, list[str]] = {}
    for row_html in re.findall(r"<tr>(.*?)</tr>", table_match.group(1), flags=re.S):
        header_match = re.search(r"<th[^>]*>(.*?)</th>", row_html, flags=re.S)
        if not header_match:
            continue
        metric = _clean_html_text(header_match.group(1))
        cells = [_clean_html_text(cell) for cell in re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.S)]
        if metric and cells:
            stats[metric] = cells
    return stats


def _extract_player_name(page_html: str, fallback: str) -> str:
    match = re.search(r"<h1>(.*?)<span", page_html, flags=re.S)
    if not match:
        return fallback
    name = _clean_html_text(match.group(1))
    return name or fallback


def _period_value(stats: dict[str, list[str]], metric: str, period_index: int, percentage: bool = False) -> str:
    cells = stats.get(metric) or []
    if not cells:
        return "-"
    if len(cells) == 4:
        return cells[min(period_index, len(cells) - 1)]
    index = period_index * 2 + (1 if percentage else 0)
    if index < len(cells):
        return cells[index]
    return cells[-1] if cells else "-"


def _wn8_label(value: float | None) -> str:
    if value is None:
        return "-"
    if value >= 2899:
        return "Super Unicum"
    if value >= 2350:
        return "Unicum"
    if value >= 1900:
        return "Great"
    if value >= 1600:
        return "Very Good"
    if value >= 1250:
        return "Good"
    if value >= 900:
        return "Average"
    if value >= 600:
        return "Below Average"
    if value >= 300:
        return "Bad"
    return "Very Bad"


@st.cache_data(ttl=60 * 60, show_spinner=False)
def fetch_wotlife_player_stats(url: str = WOTLIFE_PLAYER_URL, player_name: str = "Tonald_Drump_") -> dict[str, Any]:
    response = requests.get(url, headers={"User-Agent": "rafik-streamlit-app/1.0"}, timeout=WOTLIFE_TIMEOUT_SECONDS)
    response.raise_for_status()
    page_html = response.text

    history = _extract_stats_history(page_html)
    overall = _extract_overall_stats(page_html)
    parsed_player_name = _extract_player_name(page_html, player_name)
    latest = history.iloc[-1].to_dict() if not history.empty else {}
    wn8 = _to_float(_period_value(overall, "WN8", 0)) or _to_float(latest.get("wn8"))

    return {
        "player": parsed_player_name,
        "url": url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "wn8": wn8,
        "wn8_label": _wn8_label(wn8),
        "winrate": _to_float(_period_value(overall, "Victories", 0, percentage=True)) or _to_float(latest.get("winrate")),
        "battles": _to_float(_period_value(overall, "Battles", 0)) or _to_float(latest.get("battles")),
        "avg_tier": _to_float(_period_value(overall, "Ø Tier", 0)),
        "damage_dealt": _to_float(_period_value(overall, "Damage dealt", 0)),
        "recent_7d_battles": _period_value(overall, "Battles", 2),
        "recent_7d_winrate": _period_value(overall, "Victories", 2, percentage=True),
        "recent_30d_battles": _period_value(overall, "Battles", 3),
        "recent_30d_winrate": _period_value(overall, "Victories", 3, percentage=True),
        "overall": overall,
        "history": history,
    }
