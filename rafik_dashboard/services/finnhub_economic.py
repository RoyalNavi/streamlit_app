from datetime import date

import streamlit as st

from rafik_dashboard.services.finnhub_client import get as finnhub_get


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def get_economic_calendar(from_date: str | date, to: str | date) -> list[dict]:
    payload = finnhub_get(
        "/calendar/economic",
        params={
            "from": str(from_date),
            "to": str(to),
        },
    )
    if not isinstance(payload, dict):
        return []

    rows = payload.get("economicCalendar") or payload.get("economic") or []
    return rows if isinstance(rows, list) else []
