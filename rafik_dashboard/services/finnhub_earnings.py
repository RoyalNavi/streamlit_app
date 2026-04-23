from datetime import date

import streamlit as st

from rafik_dashboard.services.finnhub_client import get as finnhub_get


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def get_earnings_calendar(
    symbol: str | None = None,
    from_date: str | date | None = None,
    to: str | date | None = None,
    **kwargs,
) -> list[dict]:
    params = {}
    if symbol:
        params["symbol"] = str(symbol).upper().strip()
    from_value = from_date or kwargs.get("from")
    if from_value:
        params["from"] = str(from_value)
    if to:
        params["to"] = str(to)

    payload = finnhub_get("/calendar/earnings", params=params)
    if not isinstance(payload, dict):
        return []

    rows = payload.get("earningsCalendar") or payload.get("earnings") or []
    return rows if isinstance(rows, list) else []
