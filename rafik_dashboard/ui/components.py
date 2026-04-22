from html import escape

import streamlit as st


def render_section_heading(title: str, description: str | None = None) -> None:
    description_markup = ""
    if description:
        description_markup = f"<div class='section-heading-copy'>{escape(description)}</div>"
    st.html(
        f"""
        <div class="section-heading">
            <h2 class="section-heading-title">{escape(title)}</h2>
            {description_markup}
        </div>
        """
    )


def render_summary_strip(title: str, items: list[dict]) -> None:
    clean_items = [item for item in items if item.get("value") not in (None, "")]
    if not clean_items:
        return

    item_markup = []
    for item in clean_items[:5]:
        tone = item.get("tone") or "info"
        if tone not in {"success", "warning", "danger", "info"}:
            tone = "info"
        hint = item.get("hint") or ""
        item_markup.append(
            f"""
            <div class="summary-strip-item {tone}">
                <div class="summary-strip-label">{escape(str(item.get("label", "")))}</div>
                <div class="summary-strip-value">{escape(str(item.get("value", "")))}</div>
                {f'<div class="summary-strip-hint">{escape(str(hint))}</div>' if hint else ''}
            </div>
            """
        )

    st.html(
        f"""
        <section class="summary-strip">
            <div class="summary-strip-title">{escape(title)}</div>
            <div class="summary-strip-grid">{''.join(item_markup)}</div>
        </section>
        """
    )

