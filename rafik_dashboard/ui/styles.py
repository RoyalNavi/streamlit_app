import streamlit as st


APP_CSS = """
<style>
:root {
    --app-bg: #f6f8fb;
    --app-surface: #ffffff;
    --app-surface-soft: #f8fafc;
    --app-border: #dbe3ef;
    --app-border-soft: #edf2f7;
    --app-text: #111827;
    --app-muted: #64748b;
    --app-accent: #0f766e;
    --app-accent-strong: #0f5f59;
    --app-blue: #2563eb;
    --app-green: #15803d;
    --app-red: #b91c1c;
    --app-amber: #a16207;
    --app-radius: 8px;
    --app-shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
}

.stApp {
    background: var(--app-bg);
    color: var(--app-text);
}

.block-container {
    padding-top: 1.5rem;
    padding-bottom: 3rem;
    max-width: 1480px;
}

h1, h2, h3 {
    letter-spacing: 0;
    color: var(--app-text);
}

h1 {
    font-size: 2.05rem;
    line-height: 1.12;
    margin-bottom: 0.35rem;
}

h2, h3 {
    margin-top: 1.2rem;
}

p, li, span, label {
    letter-spacing: 0;
}

div[data-testid="stCaptionContainer"] {
    color: var(--app-muted);
}

.app-hero {
    background: linear-gradient(135deg, #ffffff 0%, #f8fafc 58%, #ecfdf5 100%);
    border: 1px solid var(--app-border);
    border-radius: var(--app-radius);
    padding: 24px;
    box-shadow: var(--app-shadow);
    margin-bottom: 12px;
}

.app-hero-kicker {
    color: var(--app-accent);
    font-size: 0.78rem;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 8px;
}

.app-hero-title {
    color: var(--app-text);
    font-size: 2rem;
    font-weight: 850;
    line-height: 1.12;
    margin: 0;
}

.app-hero-copy {
    color: #334155;
    font-size: 1rem;
    line-height: 1.5;
    max-width: 720px;
    margin-top: 10px;
}

.app-hero-grid {
    display: grid;
    grid-template-columns: minmax(0, 1.7fr) minmax(260px, 0.9fr);
    gap: 18px;
    align-items: stretch;
}

.app-market-panel {
    background: rgba(255, 255, 255, 0.76);
    border: 1px solid var(--app-border-soft);
    border-radius: var(--app-radius);
    padding: 14px;
    box-shadow: 0 6px 18px rgba(15, 23, 42, 0.045);
}

.app-market-title {
    color: var(--app-muted);
    font-size: 0.76rem;
    font-weight: 850;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-bottom: 10px;
}

.app-market-status {
    color: var(--app-text);
    font-size: 1.18rem;
    font-weight: 850;
    line-height: 1.2;
}

.app-market-reason {
    color: var(--app-muted);
    font-size: 0.82rem;
    line-height: 1.35;
    margin-top: 6px;
}

.app-market-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
    margin-top: 12px;
}

.app-market-chip {
    background: var(--app-surface-soft);
    border: 1px solid var(--app-border-soft);
    border-radius: 8px;
    padding: 9px 10px;
}

.app-market-chip-label {
    color: var(--app-muted);
    font-size: 0.74rem;
    font-weight: 750;
}

.app-market-chip-value {
    color: var(--app-text);
    font-size: 0.94rem;
    font-weight: 850;
    margin-top: 3px;
}

.app-system-strip {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
    background: transparent;
    color: var(--app-muted);
    font-size: 0.8rem;
    margin: 0 0 14px;
}

.app-system-pill {
    background: rgba(255, 255, 255, 0.68);
    border: 1px solid var(--app-border-soft);
    border-radius: 999px;
    padding: 6px 10px;
}

@media (max-width: 900px) {
    .app-hero-grid {
        grid-template-columns: 1fr;
    }
}

.section-heading {
    margin: 24px 0 12px;
}

.section-heading-title {
    color: var(--app-text);
    font-size: 1.28rem;
    font-weight: 850;
    line-height: 1.18;
    margin: 0;
}

.section-heading-copy {
    color: var(--app-muted);
    font-size: 0.94rem;
    line-height: 1.45;
    margin-top: 5px;
    max-width: 920px;
}

.summary-strip {
    background: var(--app-surface);
    border: 1px solid var(--app-border-soft);
    border-radius: var(--app-radius);
    box-shadow: 0 6px 18px rgba(15, 23, 42, 0.045);
    padding: 14px;
    margin: 10px 0 18px;
}

.summary-strip-title {
    color: var(--app-muted);
    font-size: 0.78rem;
    font-weight: 850;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-bottom: 10px;
}

.summary-strip-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 10px;
}

.summary-strip-item {
    background: var(--app-surface-soft);
    border: 1px solid var(--app-border-soft);
    border-left: 4px solid var(--app-border);
    border-radius: 8px;
    padding: 10px 12px;
    min-height: 76px;
}

.summary-strip-item.success {
    border-left-color: var(--app-green);
}

.summary-strip-item.warning {
    border-left-color: var(--app-amber);
}

.summary-strip-item.danger {
    border-left-color: var(--app-red);
}

.summary-strip-item.info {
    border-left-color: var(--app-blue);
}

.summary-strip-label {
    color: var(--app-muted);
    font-size: 0.78rem;
    font-weight: 750;
    margin-bottom: 5px;
}

.summary-strip-value {
    color: var(--app-text);
    font-size: 1.08rem;
    font-weight: 850;
    line-height: 1.2;
}

.summary-strip-hint {
    color: var(--app-muted);
    font-size: 0.76rem;
    line-height: 1.3;
    margin-top: 5px;
}

div[data-testid="stMetric"] {
    background: var(--app-surface);
    border: 1px solid var(--app-border-soft);
    border-radius: var(--app-radius);
    padding: 14px 16px;
    box-shadow: 0 6px 16px rgba(15, 23, 42, 0.04);
    min-height: 92px;
}

div[data-testid="stMetricLabel"] p {
    color: var(--app-muted);
    font-size: 0.82rem;
    font-weight: 700;
}

div[data-testid="stMetricValue"] {
    color: var(--app-text);
    font-weight: 800;
}

div[data-testid="stMetricDelta"] {
    font-weight: 700;
}

div[data-testid="stDataFrame"],
div[data-testid="stTable"] {
    border: 1px solid var(--app-border-soft);
    border-radius: var(--app-radius);
    overflow: hidden;
    box-shadow: 0 5px 16px rgba(15, 23, 42, 0.035);
}

div[data-testid="stExpander"] {
    background: var(--app-surface);
    border: 1px solid var(--app-border-soft);
    border-radius: var(--app-radius);
    box-shadow: 0 5px 16px rgba(15, 23, 42, 0.03);
}

div[data-testid="stTabs"] [data-baseweb="tab-list"] {
    gap: 6px;
    border-bottom: 1px solid var(--app-border);
}

div[data-testid="stTabs"] [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0;
    padding: 10px 14px;
    color: var(--app-muted);
    font-weight: 700;
}

div[data-testid="stTabs"] [aria-selected="true"] {
    color: var(--app-accent-strong);
    background: #ecfdf5;
}

div[data-testid="stRadio"] [role="radiogroup"] {
    background: var(--app-surface);
    border: 1px solid var(--app-border);
    border-radius: var(--app-radius);
    padding: 5px;
    box-shadow: 0 5px 16px rgba(15, 23, 42, 0.035);
}

div[data-testid="stRadio"] label {
    border-radius: 6px;
    padding: 7px 10px;
    font-weight: 700;
}

.stButton > button,
button[kind="primary"],
button[kind="secondary"] {
    border-radius: var(--app-radius);
    border: 1px solid var(--app-border);
    font-weight: 750;
}

.stButton > button:hover {
    border-color: var(--app-accent);
    color: var(--app-accent-strong);
}

div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div,
textarea {
    border-radius: var(--app-radius);
}

.element-container:has(.stPlotlyChart) {
    background: var(--app-surface);
    border: 1px solid var(--app-border-soft);
    border-radius: var(--app-radius);
    padding: 8px;
    box-shadow: 0 5px 16px rgba(15, 23, 42, 0.035);
}

div[data-testid="stVerticalBlock"] > div:has(> .news-card) {
    border: 1px solid var(--app-border-soft);
    border-radius: var(--app-radius);
    padding: 14px;
    background: var(--app-surface);
    box-shadow: 0 5px 16px rgba(15, 23, 42, 0.035);
}

.news-card-title {
    font-size: 1.02rem;
    line-height: 1.28;
    font-weight: 800;
    margin: 0 0 6px;
}

.news-card-title a {
    color: var(--app-text);
    text-decoration: none;
}

.news-card-title a:hover {
    color: var(--app-accent-strong);
}

.news-card-meta {
    color: var(--app-muted);
    font-size: 0.8rem;
    font-weight: 650;
    margin-bottom: 8px;
}

.news-card-summary {
    color: #334155;
    font-size: 0.92rem;
    line-height: 1.45;
    margin: 0;
}
</style>
"""


def inject_global_styles() -> None:
    """Centralise the lightweight visual system for the Streamlit UI."""
    st.markdown(APP_CSS, unsafe_allow_html=True)
