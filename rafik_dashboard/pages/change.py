import streamlit as st

from rafik_dashboard.services.exchange_rates import CHANGE_DZ_RATES_URL, fetch_changedz_rates
from rafik_dashboard.ui.components import render_section_heading, render_summary_strip


def render_change_rates_section(user_agent: str) -> None:
    render_section_heading(
        "Change",
        "Taux de change du dinar recuperes depuis changedz.fr.",
    )

    refresh_col, source_col = st.columns([1, 3])
    if refresh_col.button("Rafraichir les taux", key="refresh_changedz_rates", use_container_width=True):
        fetch_changedz_rates.clear()
    source_col.caption(f"Source : `{CHANGE_DZ_RATES_URL}`")

    with st.spinner("Je recupere les taux de change..."):
        try:
            rates_payload = fetch_changedz_rates(user_agent)
        except Exception as exc:
            st.info(f"Impossible de charger les taux de change pour le moment : {exc}")
            return

    rates_frame = rates_payload.get("frame")
    if rates_frame is None or rates_frame.empty:
        st.info("Aucun taux de change disponible dans la reponse.")
        return

    updated_at = rates_payload.get("updated_at") or "-"
    summary_items = [
        {"label": "Taux disponibles", "value": len(rates_frame), "hint": "lignes chargees", "tone": "info"},
        {"label": "Derniere MAJ", "value": updated_at, "hint": "fourni par la source", "tone": "info"},
    ]
    for _, row in rates_frame.head(3).iterrows():
        label = str(row.get("Devise") or row.get("Nom") or "Devise")
        value = row.get("Vente", row.get("Taux", row.get("Valeur", "-")))
        summary_items.append({"label": label, "value": value, "hint": "prix de vente", "tone": "success"})
    render_summary_strip("Lecture change", summary_items)

    displayed_frame = rates_frame.copy()
    if "Devise" in displayed_frame.columns:
        currency_options = ["Toutes"] + sorted(str(value) for value in displayed_frame["Devise"].dropna().unique())
        selected_currency = st.selectbox("Filtrer par devise", options=currency_options, key="changedz_currency_filter")
        if selected_currency != "Toutes":
            displayed_frame = displayed_frame[displayed_frame["Devise"].astype(str) == selected_currency]

    st.dataframe(displayed_frame, width="stretch", hide_index=True)

