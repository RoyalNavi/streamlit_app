from datetime import date

import streamlit as st

from rafik_dashboard.services.finnhub_client import get as finnhub_get


@st.cache_data(ttl=60 * 60, show_spinner=False)
def get_company_news(symbol: str, from_date: str | date, to: str | date) -> list[dict]:
    if not symbol:
        return []

    payload = finnhub_get(
        "/company-news",
        params={
            "symbol": str(symbol).upper().strip(),
            "from": str(from_date),
            "to": str(to),
        },
    )
    return payload if isinstance(payload, list) else []
