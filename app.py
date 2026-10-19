"""
QueueGuard AI — Main Streamlit Dashboard
Real-time monitoring, job submission, failure analysis, and retry management.
All metrics are sourced from live Redis/RQ data and persisted history.
"""

import sys
import os
import json
import logging
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import MAX_RETRIES, QUEUE_NAME, ALLOWED_TASKS
from queueguard.redis_connection import check_redis_health
from queueguard.job_service import (
    enqueue_job,
    get_job_status,
    list_recent_jobs,
    get_queue_stats,
    get_task_descriptions,
)
from queueguard.recovery import retry_job, calculate_backoff
from queueguard.history import list_all_jobs, load_job_attempts, clear_all_history

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Page configuration  (must be first Streamlit call)
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="QueueGuard AI",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# Theme tokens
# ──────────────────────────────────────────────────────────────────────────────

# Bootstrap theme from session state (persists across reruns)
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

# All design tokens for dark mode
DARK: dict = {
    "bg":            "linear-gradient(135deg,#0f0f1a 0%,#1a1a2e 50%,#16213e 100%)",
    "bg_solid":      "#0f0f1a",
    "surface":       "rgba(255,255,255,0.05)",
    "surface2":      "rgba(255,255,255,0.04)",
    "border":        "rgba(255,255,255,0.10)",
    "border2":       "rgba(255,255,255,0.06)",
    "text":          "#e2e8f0",
    "text_strong":   "#f1f5f9",
    "muted":         "#94a3b8",
    "accent":        "#6366f1",
    "accent2":       "#8b5cf6",
    "accent3":       "#06b6d4",
    "sidebar_bg":    "rgba(15,15,26,0.97)",
    "tab_bg":        "rgba(255,255,255,0.03)",
    "tab_active_bg": "rgba(99,102,241,0.30)",
    "tab_active_fg": "#e0e7ff",
    "tab_fg":        "#94a3b8",
    "input_bg":      "rgba(255,255,255,0.06)",
    "input_text":    "#f1f5f9",
    "shadow":        "0 10px 30px rgba(0,0,0,0.45)",
    "chart_paper":   "rgba(0,0,0,0)",
    "chart_plot":    "rgba(0,0,0,0)",
    "chart_font":    "#e2e8f0",
    "chart_grid":    "rgba(255,255,255,0.08)",
    "metric_value":  "#f1f5f9",
    "metric_label":  "#94a3b8",
    "badge_queued_bg":   "rgba(251,191,36,0.18)",
    "badge_queued_fg":   "#fbbf24",
    "badge_started_bg":  "rgba(99,102,241,0.18)",
    "badge_started_fg":  "#818cf8",
    "badge_finished_bg": "rgba(34,197,94,0.18)",
    "badge_finished_fg": "#4ade80",
    "badge_failed_bg":   "rgba(239,68,68,0.18)",
    "badge_failed_fg":   "#f87171",
    "badge_retrying_bg": "rgba(249,115,22,0.18)",
    "badge_retrying_fg": "#fb923c",
    "badge_dead_bg":     "rgba(100,116,139,0.18)",
    "badge_dead_fg":     "#94a3b8",
    "sev_critical":  "#f87171",
    "sev_high":      "#fb923c",
    "sev_medium":    "#fbbf24",
    "sev_low":       "#4ade80",
    "btn_bg":        "linear-gradient(135deg,#6366f1,#8b5cf6)",
    "btn_fg":        "#ffffff",
    "divider":       "rgba(255,255,255,0.08)",
    "empty_border":  "rgba(255,255,255,0.12)",
    # highlight_status background/foreground pairs (for dataframe rows)
    "hl_queued_bg":   "rgba(251,191,36,0.12)",
    "hl_queued_fg":   "#fbbf24",
    "hl_started_bg":  "rgba(99,102,241,0.12)",
    "hl_started_fg":  "#818cf8",
    "hl_finished_bg": "rgba(34,197,94,0.12)",
    "hl_finished_fg": "#4ade80",
    "hl_failed_bg":   "rgba(239,68,68,0.12)",
    "hl_failed_fg":   "#f87171",
    "hl_retrying_bg": "rgba(249,115,22,0.12)",
    "hl_retrying_fg": "#fb923c",
    "hl_dead_bg":     "rgba(100,116,139,0.12)",
    "hl_dead_fg":     "#94a3b8",
}

# All design tokens for light mode
LIGHT: dict = {
    "bg":            "linear-gradient(135deg,#f1f5f9 0%,#e8eef7 50%,#dde6f0 100%)",
    "bg_solid":      "#f1f5f9",
    "surface":       "rgba(255,255,255,0.90)",
    "surface2":      "#ffffff",
    "border":        "rgba(99,102,241,0.20)",
    "border2":       "rgba(0,0,0,0.07)",
    "text":          "#1e293b",
    "text_strong":   "#0f172a",
    "muted":         "#64748b",
    "accent":        "#6366f1",
    "accent2":       "#7c3aed",
    "accent3":       "#0891b2",
    "sidebar_bg":    "#ffffff",
    "tab_bg":        "rgba(99,102,241,0.07)",
    "tab_active_bg": "rgba(99,102,241,0.18)",
    "tab_active_fg": "#4338ca",
    "tab_fg":        "#475569",
    "input_bg":      "#ffffff",
    "input_text":    "#1e293b",
    "shadow":        "0 10px 30px rgba(99,102,241,0.14)",
    "chart_paper":   "rgba(0,0,0,0)",
    "chart_plot":    "rgba(248,250,252,0.95)",
    "chart_font":    "#1e293b",
    "chart_grid":    "rgba(0,0,0,0.06)",
    "metric_value":  "#0f172a",
    "metric_label":  "#64748b",
    "badge_queued_bg":   "rgba(217,119,6,0.13)",
    "badge_queued_fg":   "#92400e",
    "badge_started_bg":  "rgba(99,102,241,0.13)",
    "badge_started_fg":  "#3730a3",
    "badge_finished_bg": "rgba(22,163,74,0.13)",
    "badge_finished_fg": "#14532d",
    "badge_failed_bg":   "rgba(220,38,38,0.13)",
    "badge_failed_fg":   "#7f1d1d",
    "badge_retrying_bg": "rgba(234,88,12,0.13)",
    "badge_retrying_fg": "#7c2d12",
    "badge_dead_bg":     "rgba(100,116,139,0.13)",
    "badge_dead_fg":     "#334155",
    "sev_critical":  "#dc2626",
    "sev_high":      "#ea580c",
    "sev_medium":    "#ca8a04",
    "sev_low":       "#16a34a",
    "btn_bg":        "linear-gradient(135deg,#6366f1,#7c3aed)",
    "btn_fg":        "#ffffff",
    "divider":       "rgba(0,0,0,0.08)",
    "empty_border":  "rgba(99,102,241,0.20)",
    "hl_queued_bg":   "rgba(217,119,6,0.10)",
    "hl_queued_fg":   "#92400e",
    "hl_started_bg":  "rgba(99,102,241,0.10)",
    "hl_started_fg":  "#3730a3",
    "hl_finished_bg": "rgba(22,163,74,0.10)",
    "hl_finished_fg": "#14532d",
    "hl_failed_bg":   "rgba(220,38,38,0.10)",
    "hl_failed_fg":   "#7f1d1d",
    "hl_retrying_bg": "rgba(234,88,12,0.10)",
    "hl_retrying_fg": "#7c2d12",
    "hl_dead_bg":     "rgba(100,116,139,0.10)",
    "hl_dead_fg":     "#334155",
}


def _T() -> dict:
    """Return the active token dict."""
    return DARK if st.session_state.get("theme", "dark") == "dark" else LIGHT


# ──────────────────────────────────────────────────────────────────────────────
# CSS injection
# ──────────────────────────────────────────────────────────────────────────────

def inject_css() -> None:
    """Inject all theme-aware CSS. Called once per render cycle."""
    T = _T()
    st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

  /* ── Base typography ── */
  html, body, [class*="css"] {{
    font-family: 'Inter', sans-serif;
    color: {T['text']};
  }}
  .stApp p, .stApp li, .stApp span {{ color: {T['text']}; }}
  .stApp h1, .stApp h2, .stApp h3,
  .stApp h4, .stApp h5, .stApp h6 {{ color: {T['text_strong']}; }}
  .stMarkdown p {{ color: {T['text']}; }}
  .stCaption p, [data-testid="stCaptionContainer"] p {{
    color: {T['muted']} !important; font-size: 0.83rem;
  }}

  /* ── Background ── */
  .stApp {{ background: {T['bg']}; }}

  /* ── Sidebar ── */
  [data-testid="stSidebar"] {{
    background: {T['sidebar_bg']} !important;
    border-right: 1px solid {T['border2']};
  }}
  [data-testid="stSidebar"] p,
  [data-testid="stSidebar"] span,
  [data-testid="stSidebar"] label,
  [data-testid="stSidebar"] .stMarkdown p {{ color: {T['text']} !important; }}
  [data-testid="stSidebar"] h1,
  [data-testid="stSidebar"] h2,
  [data-testid="stSidebar"] h3 {{ color: {T['text_strong']} !important; }}
  [data-testid="stSidebar"] .stCaption p {{ color: {T['muted']} !important; }}

  /* ── Tabs ── */
  .stTabs [data-baseweb="tab-list"] {{
    gap: 6px;
    background: {T['tab_bg']};
    border-radius: 10px;
    padding: 4px;
    border: 1px solid {T['border']};
  }}
  .stTabs [data-baseweb="tab"] {{
    border-radius: 8px;
    color: {T['tab_fg']} !important;
    font-weight: 500;
    padding: 0.4rem 0.75rem;
  }}
  .stTabs [aria-selected="true"] {{
    background: {T['tab_active_bg']} !important;
    color: {T['tab_active_fg']} !important;
    font-weight: 600;
  }}

  /* ── Inputs ── */
  .stTextInput input,
  .stNumberInput input,
  .stTextArea textarea {{
    background: {T['input_bg']} !important;
    color: {T['input_text']} !important;
    border-color: {T['border']} !important;
    border-radius: 8px;
  }}
  [data-testid="stSelectbox"] div[data-baseweb="select"] div,
  [data-testid="stSelectbox"] span {{
    background: {T['input_bg']} !important;
    color: {T['input_text']} !important;
  }}
  .stMultiSelect [data-baseweb="tag"] {{
    background: {T['tab_active_bg']} !important;
    color: {T['tab_active_fg']} !important;
  }}

  /* ── Metric containers (st.metric) ── */
  [data-testid="metric-container"] {{
    background: {T['surface']};
    border: 1px solid {T['border']};
    border-radius: 12px;
    padding: 1rem;
    backdrop-filter: blur(8px);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
  }}
  [data-testid="metric-container"]:hover {{
    transform: translateY(-2px);
    box-shadow: {T['shadow']};
  }}
  [data-testid="stMetricValue"] {{
    color: {T['metric_value']} !important;
    font-size: 1.8rem !important;
    font-weight: 700 !important;
  }}
  [data-testid="stMetricLabel"] {{
    color: {T['metric_label']} !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }}

  /* ── Custom HTML metric cards ── */
  .qg-metric-card {{
    background: {T['surface']};
    border: 1px solid {T['border']};
    border-radius: 14px;
    padding: 1.1rem 1rem 1rem;
    text-align: center;
    backdrop-filter: blur(8px);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    position: relative;
    overflow: hidden;
  }}
  .qg-metric-card::before {{
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 3px;
    border-radius: 14px 14px 0 0;
    background: var(--qg-accent, {T['accent']});
  }}
  .qg-metric-card:hover {{
    transform: translateY(-3px);
    box-shadow: {T['shadow']};
  }}
  .qg-metric-icon  {{ font-size: 1.3rem; line-height: 1; margin-bottom: 0.25rem; }}
  .qg-metric-value {{
    font-size: 2rem; font-weight: 800; line-height: 1.1;
    color: var(--qg-value-color, {T['text_strong']});
    letter-spacing: -0.02em;
  }}
  .qg-metric-label {{
    font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.06em; color: {T['muted']}; margin-top: 0.3rem;
  }}

  /* ── App header ── */
  .qg-header {{
    background: linear-gradient(90deg, {T['accent']} 0%, {T['accent2']} 55%, {T['accent3']} 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-size: 2.4rem; font-weight: 800; margin-bottom: 0; line-height: 1.1;
  }}
  .qg-subtitle {{ color: {T['muted']}; font-size: 1rem; margin-top: 0.3rem; }}

  /* ── Status badges ── */
  .badge {{
    display: inline-block; padding: 3px 10px; border-radius: 99px;
    font-size: 0.73rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em;
  }}
  .badge-queued   {{ background:{T['badge_queued_bg']};   color:{T['badge_queued_fg']}; }}
  .badge-started  {{ background:{T['badge_started_bg']};  color:{T['badge_started_fg']}; }}
  .badge-finished {{ background:{T['badge_finished_bg']}; color:{T['badge_finished_fg']}; }}
  .badge-failed   {{ background:{T['badge_failed_bg']};   color:{T['badge_failed_fg']}; }}
  .badge-retrying {{ background:{T['badge_retrying_bg']}; color:{T['badge_retrying_fg']}; }}
  .badge-dead     {{ background:{T['badge_dead_bg']};     color:{T['badge_dead_fg']}; }}

  /* ── Content cards ── */
  .qg-card {{
    background: {T['surface2']};
    border: 1px solid {T['border']};
    border-radius: 14px; padding: 1.2rem; margin-bottom: 1rem;
  }}
  .qg-card p, .qg-card span {{ color: {T['text']}; }}

  /* ── Severity ── */
  .sev-critical {{ color:{T['sev_critical']}; font-weight:700; }}
  .sev-high     {{ color:{T['sev_high']};     font-weight:700; }}
  .sev-medium   {{ color:{T['sev_medium']};   font-weight:600; }}
  .sev-low      {{ color:{T['sev_low']};      font-weight:600; }}

  /* ── Buttons ── */
  .stButton > button {{
    background: {T['btn_bg']} !important;
    color: {T['btn_fg']} !important;
    border: none; border-radius: 8px;
    font-weight: 600; font-size: 0.9rem;
    transition: all 0.2s ease;
  }}
  .stButton > button:hover {{
    transform: translateY(-1px);
    box-shadow: 0 4px 15px rgba(99,102,241,0.35);
    filter: brightness(1.08);
  }}

  /* ── Redis offline banner ── */
  .redis-offline {{
    background: rgba(239,68,68,0.12);
    border: 1px solid rgba(239,68,68,0.35);
    border-radius: 10px; padding: 1rem; text-align: center;
  }}

  /* ── Expanders ── */
  [data-testid="stExpander"] summary {{
    color: {T['text']} !important;
    font-weight: 500;
  }}
  [data-testid="stExpander"] summary:hover {{
    color: {T['accent']} !important;
  }}
  [data-testid="stExpander"] .stMarkdown p {{
    color: {T['text']} !important;
  }}

  /* ── Dataframe / table ── */
  [data-testid="stDataFrame"] thead th {{
    color: {T['text_strong']} !important;
    font-weight: 600 !important;
    background: {T['surface']} !important;
    border-bottom: 2px solid {T['border']} !important;
  }}
  [data-testid="stDataFrame"] tbody td {{
    color: {T['text']} !important;
  }}

  /* ── Slider ── */
  [data-testid="stSlider"] label {{ color: {T['text']} !important; }}

  /* ── Toggle ── */
  [data-testid="stToggle"] label {{ color: {T['text']} !important; }}

  /* ── Divider ── */
  hr {{ border-color: {T['divider']}; opacity: 1; margin: 1rem 0; }}

  /* ── Code ── */
  .stCode code, pre {{ font-size: 0.82rem; border-radius: 8px !important; }}

  /* ── Empty state ── */
  .qg-empty {{
    text-align: center; padding: 3rem 1rem;
    color: {T['muted']};
    border: 1px dashed {T['empty_border']};
    border-radius: 14px;
    margin: 1rem 0;
  }}
  .qg-empty h3 {{ color: {T['muted']}; margin-bottom: 0.5rem; }}
  .qg-empty p  {{ color: {T['muted']}; margin: 0.25rem 0; }}

  /* ── JSON display ── */
  [data-testid="stJson"] {{ color: {T['text']} !important; }}
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# Plotly helper
# ──────────────────────────────────────────────────────────────────────────────

def chart_layout(extra: dict | None = None) -> dict:
    """Return a theme-aware Plotly layout dict."""
    T = _T()
    base = dict(
        paper_bgcolor=T["chart_paper"],
        plot_bgcolor=T["chart_plot"],
        font=dict(color=T["chart_font"], family="Inter, sans-serif", size=12),
        xaxis=dict(
            gridcolor=T["chart_grid"],
            tickfont=dict(color=T["chart_font"]),
            linecolor=T["chart_grid"],
            zeroline=False,
        ),
        yaxis=dict(
            gridcolor=T["chart_grid"],
            tickfont=dict(color=T["chart_font"]),
            linecolor=T["chart_grid"],
            zeroline=False,
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=T["chart_font"]),
            bordercolor="rgba(0,0,0,0)",
        ),
        margin=dict(t=30, b=40, l=40, r=20),
    )
    if extra:
        base.update(extra)
    return base


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────

def _status_badge(status: str) -> str:
    return f'<span class="badge badge-{status}">{status}</span>'


def _severity_span(severity: str) -> str:
    return f'<span class="sev-{severity}">{severity.upper()}</span>'


def _sev_icon(sev: str) -> str:
    return {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(sev, "⚪")


def _fmt_dt(iso_str: str | None) -> str:
    if not iso_str:
        return "—"
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%H:%M:%S")
    except Exception:
        return str(iso_str)[:19]


def _fmt_duration(dur: float | None) -> str:
    if dur is None:
        return "—"
    if dur < 1:
        return f"{dur*1000:.0f}ms"
    return f"{dur:.2f}s"


def _get_redis_client_safe():
    """Return decoded Redis client or None (for history reads)."""
    try:
        from queueguard.redis_connection import get_redis_client
        r = get_redis_client()
        r.ping()
        return r
    except Exception:
        return None


def highlight_status(val: str) -> str:
    """Return theme-aware Pandas Styler CSS for a status cell."""
    T = _T()
    key_bg = f"hl_{val}_bg"
    key_fg = f"hl_{val}_fg"
    if key_bg in T and key_fg in T:
        return f"background-color:{T[key_bg]};color:{T[key_fg]};font-weight:600"
    return ""


# ──────────────────────────────────────────────────────────────────────────────
# Redis connection guard
# ──────────────────────────────────────────────────────────────────────────────

def render_redis_status_banner(health: dict) -> bool:
    """Render connection banner. Returns True if Redis is connected."""
    if health["connected"]:
        return True

    st.markdown("""
    <div class="redis-offline">
      <h3 style="color:#f87171;margin:0">⛔ Redis Unavailable</h3>
      <p style="color:#fca5a5;margin:0.5rem 0 0">
        Cannot connect to Redis. The dashboard cannot display live job data.
      </p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("🔧 How to fix this", expanded=True):
        st.markdown(f"""
**Error:** `{health.get('error', 'Unknown')}`

**Redis endpoint:** `{health['host']}:{health['port']}`

**Fix options:**

1. **Docker (recommended):**
   ```powershell
   docker-compose up -d
   ```

2. **Check if Redis is running:**
   ```powershell
   docker ps | findstr redis
   ```

3. **Test connectivity manually:**
   ```powershell
   docker exec queueguard-redis redis-cli ping
   ```

4. **Configure a different host** by editing `.env`:
   ```
   REDIS_HOST=your-redis-host
   REDIS_PORT=6379
   ```
        """)
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────

def render_sidebar(health: dict, stats: dict) -> None:
    T = _T()
    with st.sidebar:
        st.markdown(
            f'<h2 style="color:{T["text_strong"]};margin-bottom:0">🛡️ QueueGuard AI</h2>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<p style="color:{T["muted"]};font-size:0.85rem;margin-top:0.2rem">'
            "AI-Powered Job Monitor</p>",
            unsafe_allow_html=True,
        )
        st.divider()

        # ── Theme selector ──────────────────────────────────────────────────
        st.markdown(
            f'<p style="color:{T["muted"]};font-size:0.78rem;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.06em;margin-bottom:0.3rem">'
            "💡 Theme</p>",
            unsafe_allow_html=True,
        )
        theme_choice = st.selectbox(
            "Theme",
            options=["dark", "light"],
            format_func=lambda x: "🌙 Dark Mode" if x == "dark" else "☀️ Light Mode",
            index=0 if st.session_state.theme == "dark" else 1,
            key="theme_select",
            label_visibility="collapsed",
        )
        if theme_choice != st.session_state.theme:
            st.session_state.theme = theme_choice
            st.rerun()

        st.divider()

        # ── Connection status ───────────────────────────────────────────────
        if health["connected"]:
            st.success(
                f"✅ Redis Connected\n\n"
                f"`{health['host']}:{health['port']}` · {health.get('latency_ms', '?')}ms"
            )
        else:
            st.error("❌ Redis Offline")

        st.divider()

        # ── Queue stats summary ─────────────────────────────────────────────
        st.markdown(
            f'<p style="color:{T["text_strong"]};font-weight:700;font-size:0.95rem">'
            "📊 Queue Summary</p>",
            unsafe_allow_html=True,
        )
        _strong = T["text_strong"]
        st.markdown(
            f'<p style="color:{T["muted"]};font-size:0.82rem;margin:0">'
            f"Queue: <strong style='color:{_strong}'>{QUEUE_NAME}</strong></p>",
            unsafe_allow_html=True,
        )
        if health["connected"]:
            col1, col2 = st.columns(2)
            col1.metric("Active", stats.get("started", 0))
            col2.metric("Waiting", stats.get("queued", 0))

        st.divider()

        # ── Auto-refresh ────────────────────────────────────────────────────
        st.markdown(
            f'<p style="color:{T["text_strong"]};font-weight:700;font-size:0.95rem">'
            "🔄 Auto-Refresh</p>",
            unsafe_allow_html=True,
        )
        auto_refresh = st.toggle("Enable", value=False, key="auto_refresh_toggle")
        if auto_refresh:
            interval = st.slider("Interval (seconds)", 5, 60, 10, key="refresh_interval")
            try:
                from streamlit_autorefresh import st_autorefresh
                st_autorefresh(interval=interval * 1000, key="auto_refresher")
                st.caption(f"Refreshing every {interval}s")
            except ImportError:
                st.warning("Install `streamlit-autorefresh` for auto-refresh.")

        st.divider()

        if st.button("🔃 Refresh Now", use_container_width=True):
            st.rerun()

        st.divider()
        st.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem">'
            f"Max retries: {MAX_RETRIES} · v1.0.0</p>",
            unsafe_allow_html=True,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Tab 1: Overview
# ──────────────────────────────────────────────────────────────────────────────

def render_overview(stats: dict) -> None:
    T = _T()
    st.markdown(
        f'<h3 style="color:{T["text_strong"]}">📈 Real-Time Queue Metrics</h3>',
        unsafe_allow_html=True,
    )
    st.caption("Sourced from live Redis/RQ data + persisted job history.")

    # Metric cards – values come directly from live stats dict
    metrics = [
        {"icon": "💼", "label": "Total Jobs",  "value": stats.get("total", 0),
         "accent": "#6366f1", "color": T["metric_value"]},
        {"icon": "⏳", "label": "Queued",      "value": stats.get("queued", 0),
         "accent": "#f59e0b", "color": "#fbbf24" if st.session_state.theme == "dark" else "#92400e"},
        {"icon": "⚡", "label": "Running",     "value": stats.get("started", 0),
         "accent": "#8b5cf6", "color": "#ddd6fe" if st.session_state.theme == "dark" else "#4338ca"},
        {"icon": "✅", "label": "Completed",   "value": stats.get("finished", 0),
         "accent": "#10b981", "color": "#6ee7b7" if st.session_state.theme == "dark" else "#14532d"},
        {"icon": "❌", "label": "Failed",      "value": stats.get("failed", 0),
         "accent": "#ef4444", "color": "#fca5a5" if st.session_state.theme == "dark" else "#7f1d1d"},
        {"icon": "🔁", "label": "Retrying",    "value": stats.get("retrying", 0),
         "accent": "#f97316", "color": "#fdba74" if st.session_state.theme == "dark" else "#7c2d12"},
        {"icon": "🛡️", "label": "Recovery %", "value": f"{stats.get('recovery_rate', 0)}%",
         "accent": "#06b6d4", "color": "#67e8f9" if st.session_state.theme == "dark" else "#0e7490"},
    ]

    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        col.markdown(
            f"""
            <div class="qg-metric-card"
                 style="--qg-accent:{m['accent']};--qg-value-color:{m['color']}">
              <div class="qg-metric-icon">{m['icon']}</div>
              <div class="qg-metric-value">{m['value']}</div>
              <div class="qg-metric-label">{m['label']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Dead-letter warning (real count from stats)
    dead = stats.get("dead", 0)
    if dead > 0:
        st.warning(
            f"⚠️ **{dead} job(s) in dead-letter queue** — max retries exceeded. "
            "Review in the Failed Jobs tab."
        )

    # Recent activity table
    st.markdown("---")
    st.markdown(
        f'<h4 style="color:{T["text_strong"]}">🕐 Recent Activity</h4>',
        unsafe_allow_html=True,
    )
    jobs = list_recent_jobs(limit=10)
    if not jobs:
        st.markdown(
            '<div class="qg-empty"><h3>📭 No jobs yet</h3>'
            '<p>Submit a job from the <strong>Submit Job</strong> tab.</p></div>',
            unsafe_allow_html=True,
        )
    else:
        rows = []
        for job in jobs[:10]:
            rows.append({
                "Task": job.get("task_name", "—").replace("_", " "),
                "Status": job.get("status", "—"),
                "Enqueued": _fmt_dt(job.get("enqueue_time")),
                "Duration": _fmt_duration(job.get("duration_s")),
                "Retries": job.get("retry_count", 0),
            })
        df = pd.DataFrame(rows)
        styled = df.style.map(highlight_status, subset=["Status"])
        st.dataframe(styled, use_container_width=True, hide_index=True)


# ──────────────────────────────────────────────────────────────────────────────
# Tab 2: Submit Job
# ──────────────────────────────────────────────────────────────────────────────

def render_submit_job() -> None:
    T = _T()
    st.markdown(
        f'<h3 style="color:{T["text_strong"]}">➕ Submit a New Job</h3>',
        unsafe_allow_html=True,
    )
    st.caption("All jobs run in the RQ worker process. Parameters are validated before enqueuing.")

    task_descriptions = get_task_descriptions()
    task_names = list(task_descriptions.keys())

    col_form, col_info = st.columns([3, 2])

    with col_form:
        with st.form("job_submit_form", clear_on_submit=False):
            task_name = st.selectbox(
                "Task Type",
                task_names,
                format_func=lambda t: {
                    "successful_task":     "✅ Successful Task",
                    "failing_task":        "❌ Failing Task (test failures)",
                    "slow_task":           "⏳ Slow Task (long-running)",
                    "data_processing_task":"📊 Data Processing Task",
                }.get(t, t),
                key="task_select",
            )

            params: dict = {}

            if task_name == "successful_task":
                params["duration"] = st.slider("Duration (seconds)", 0.1, 10.0, 1.0, 0.1)
                params["payload"] = st.text_input("Payload", value="hello", max_chars=256)

            elif task_name == "failing_task":
                params["error_type"] = st.selectbox(
                    "Error Type",
                    ["timeout", "connection", "value", "runtime"],
                    format_func=lambda e: {
                        "timeout":    "⏱️ Timeout Error",
                        "connection": "🔌 Connection Error",
                        "value":      "🔢 Value/Input Error",
                        "runtime":    "💥 Runtime Error",
                    }.get(e, e),
                )

            elif task_name == "slow_task":
                params["duration"] = st.slider("Duration (seconds)", 1.0, 30.0, 5.0, 1.0)
                st.info("💡 Tip: Start the worker before submitting so the job executes.")

            elif task_name == "data_processing_task":
                params["dataset_size"] = st.number_input(
                    "Dataset Size (records)", min_value=10, max_value=10_000,
                    value=1_000, step=100
                )

            submitted = st.form_submit_button("🚀 Submit Job", use_container_width=True)

    with col_info:
        st.markdown(
            f'<p style="color:{T["text_strong"]};font-weight:700">ℹ️ Task Description</p>',
            unsafe_allow_html=True,
        )
        st.info(task_descriptions.get(task_name, ""))
        st.markdown(
            f'<p style="color:{T["text_strong"]};font-weight:700">⚙️ Parameters</p>',
            unsafe_allow_html=True,
        )
        st.json(params)

    if submitted:
        with st.spinner("Enqueuing job…"):
            result = enqueue_job(task_name, params)

        if result["success"]:
            st.success("✅ Job enqueued successfully!")
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Job ID", result["job_id"][:8] + "…")
            col_b.metric("Queue Position", result.get("queue_position", "?"))
            col_c.metric("Task", task_name.replace("_", " ").title())

            st.markdown(
                f'<p style="color:{T["text_strong"]};font-weight:600">Full Job ID:</p>',
                unsafe_allow_html=True,
            )
            st.code(result["job_id"])

            if "submitted_jobs" not in st.session_state:
                st.session_state.submitted_jobs = []
            st.session_state.submitted_jobs.insert(0, result["job_id"])

            st.info(
                "🔄 Switch to the **Job Monitor** tab to track execution. "
                "Make sure a worker is running: `python scripts/start_worker.py`"
            )
        else:
            st.error(f"❌ Job submission failed: {result['error']}")

    # Quick status for recently submitted jobs
    if "submitted_jobs" in st.session_state and st.session_state.submitted_jobs:
        st.divider()
        st.markdown(
            f'<h4 style="color:{T["text_strong"]}">🔍 Recently Submitted Jobs</h4>',
            unsafe_allow_html=True,
        )
        for jid in st.session_state.submitted_jobs[:5]:
            status_info = get_job_status(jid)
            if status_info:
                status = status_info.get("status", "unknown")
                task   = status_info.get("task_name", "—")
                cols   = st.columns([4, 2, 1])
                cols[0].markdown(f"`{jid[:20]}…`")
                cols[1].markdown(
                    f'<span style="color:{T["text"]}">{task}</span>',
                    unsafe_allow_html=True,
                )
                cols[2].markdown(_status_badge(status), unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# Tab 3: Job Monitor
# ──────────────────────────────────────────────────────────────────────────────

def render_job_monitor() -> None:
    T = _T()
    st.markdown(
        f'<h3 style="color:{T["text_strong"]}">📋 Job Execution Monitor</h3>',
        unsafe_allow_html=True,
    )
    st.caption("Live view from Redis + persisted history. All data is real.")

    col_filter, col_limit = st.columns([3, 1])
    with col_filter:
        status_filter = st.multiselect(
            "Filter by Status",
            ["queued", "started", "finished", "failed", "retrying", "dead"],
            default=[],
            key="monitor_filter",
        )
    with col_limit:
        limit = st.selectbox("Show", [25, 50, 100], key="monitor_limit")

    jobs = list_recent_jobs(limit=limit)

    if not jobs:
        st.markdown(
            '<div class="qg-empty">'
            "<h3>📭 No jobs yet</h3>"
            "<p>Submit a job from the <strong>Submit Job</strong> tab to get started.</p>"
            "<p>Make sure a worker is running: <code>python scripts/start_worker.py</code></p>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    if status_filter:
        jobs = [j for j in jobs if j.get("status") in status_filter]

    if not jobs:
        st.info("No jobs match the selected filter.")
        return

    # Build display table
    rows = []
    for job in jobs:
        rows.append({
            "Job ID":   job.get("job_id", "—")[:12] + "…",
            "Task":     job.get("task_name", "—").replace("_", " "),
            "Status":   job.get("status", "—"),
            "Enqueued": _fmt_dt(job.get("enqueue_time")),
            "Started":  _fmt_dt(job.get("start_time")),
            "Finished": _fmt_dt(job.get("finish_time")),
            "Duration": _fmt_duration(job.get("duration_s")),
            "Retries":  job.get("retry_count", 0),
            "Recovered": "✅" if job.get("recovered") else "",
        })

    df = pd.DataFrame(rows)
    styled = df.style.map(highlight_status, subset=["Status"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Job detail expander
    st.markdown(
        f'<h4 style="color:{T["text_strong"]}">🔍 Job Details</h4>',
        unsafe_allow_html=True,
    )
    job_ids   = [j.get("job_id", "") for j in jobs]
    short_ids = [jid[:20] + "…" for jid in job_ids]

    if job_ids:
        selected_short = st.selectbox("Select job", short_ids, key="detail_select")
        selected_idx   = short_ids.index(selected_short)
        _render_job_detail(jobs[selected_idx])


def _render_job_detail(job: dict) -> None:
    """Render detailed info for a single job."""
    T = _T()
    with st.container():
        st.markdown('<div class="qg-card">', unsafe_allow_html=True)

        col1, col2, col3 = st.columns(3)
        col1.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">JOB ID</p>'
            f'<p style="color:{T["text_strong"]};font-weight:600;margin:0">'
            f'<code>{job.get("job_id", "—")}</code></p>',
            unsafe_allow_html=True,
        )
        col2.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">TASK</p>'
            f'<p style="color:{T["text_strong"]};font-weight:600;margin:0">'
            f'{job.get("task_name", "—")}</p>',
            unsafe_allow_html=True,
        )
        status = job.get("status", "unknown")
        col3.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">STATUS</p>'
            f'<p style="margin:0">{_status_badge(status)}</p>',
            unsafe_allow_html=True,
        )

        col4, col5, col6 = st.columns(3)
        col4.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">ENQUEUED</p>'
            f'<p style="color:{T["text"]};margin:0">{_fmt_dt(job.get("enqueue_time"))}</p>',
            unsafe_allow_html=True,
        )
        col5.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">STARTED</p>'
            f'<p style="color:{T["text"]};margin:0">{_fmt_dt(job.get("start_time"))}</p>',
            unsafe_allow_html=True,
        )
        col6.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">FINISHED</p>'
            f'<p style="color:{T["text"]};margin:0">{_fmt_dt(job.get("finish_time"))}</p>',
            unsafe_allow_html=True,
        )

        col7, col8, col9 = st.columns(3)
        col7.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">DURATION</p>'
            f'<p style="color:{T["text"]};margin:0">{_fmt_duration(job.get("duration_s"))}</p>',
            unsafe_allow_html=True,
        )
        col8.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">RETRIES</p>'
            f'<p style="color:{T["text"]};margin:0">{job.get("retry_count", 0)} / {MAX_RETRIES}</p>',
            unsafe_allow_html=True,
        )
        col9.markdown(
            f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">RECOVERED</p>'
            f'<p style="color:{T["text"]};margin:0">'
            f'{"✅ Yes" if job.get("recovered") else "No"}</p>',
            unsafe_allow_html=True,
        )

        if job.get("task_params"):
            with st.expander("📥 Input Parameters"):
                st.json(job["task_params"])

        if job.get("result"):
            with st.expander("📤 Result"):
                result = job["result"]
                if isinstance(result, dict):
                    st.json(result)
                else:
                    st.write(result)

        if job.get("failure_analysis"):
            fa = job["failure_analysis"]
            if isinstance(fa, str):
                try:
                    fa = json.loads(fa)
                except Exception:
                    fa = {}
            if fa:
                with st.expander("🔬 Failure Analysis", expanded=True):
                    sev = fa.get("severity", "unknown")
                    st.markdown(
                        f"**Category:** `{fa.get('category', '—')}`  |  "
                        f"**Severity:** {_severity_span(sev)}  |  "
                        f"**Retryable:** {'✅' if fa.get('is_retryable') else '❌'}",
                        unsafe_allow_html=True,
                    )
                    st.markdown(f"> ⚠️ *{fa.get('summary', '')}*")
                    st.markdown(f"**Explanation:** {fa.get('explanation', '')}")
                    suggestions = fa.get("suggestions", [])
                    if suggestions:
                        st.markdown("**Suggested Actions:**")
                        for s in suggestions:
                            st.markdown(f"- {s}")
                    with st.expander("🔎 Traceback (Diagnostic Only)"):
                        st.code(fa.get("traceback_summary", "None"), language="python")

        st.markdown("</div>", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# Tab 4: Failed Jobs & Recovery
# ──────────────────────────────────────────────────────────────────────────────

def render_failed_jobs() -> None:
    T = _T()
    st.markdown(
        f'<h3 style="color:{T["text_strong"]}">❌ Failed Jobs & Recovery</h3>',
        unsafe_allow_html=True,
    )
    st.caption(
        "Failed jobs are detected from real RQ registry data. "
        "Failure analysis is a local diagnostic suggestion — not a guaranteed root cause."
    )

    jobs   = list_recent_jobs(limit=200)
    failed = [j for j in jobs if j.get("status") in ("failed", "dead", "retrying")]

    if not failed:
        st.success("🎉 No failed jobs! All recent jobs completed successfully.")
        return

    st.markdown(
        f'<p style="color:{T["text"]};font-weight:600">'
        f"{len(failed)} job(s) with issues found.</p>",
        unsafe_allow_html=True,
    )

    r = _get_redis_client_safe()

    for job in failed:
        job_id     = job.get("job_id", "")
        task_name  = job.get("task_name", "—")
        status     = job.get("status", "—")
        retry_count = int(job.get("retry_count", 0))

        fa = job.get("failure_analysis") or {}
        if isinstance(fa, str):
            try:
                fa = json.loads(fa)
            except Exception:
                fa = {}

        sev      = fa.get("severity", "unknown") if fa else "unknown"
        category = fa.get("category", "unknown") if fa else "unknown"

        with st.expander(
            f"{_sev_icon(sev)} `{job_id[:16]}…` · **{task_name}** · "
            f"{status.upper()} · Retries: {retry_count}/{MAX_RETRIES}",
            expanded=False,
        ):
            col1, col2, col3 = st.columns(3)
            col1.markdown(
                f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">ERROR TYPE</p>'
                f'<p style="color:{T["text_strong"]};margin:0"><code>{job.get("exc_type", "—")}</code></p>',
                unsafe_allow_html=True,
            )
            col2.markdown(
                f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">CATEGORY</p>'
                f'<p style="color:{T["text_strong"]};margin:0"><code>{category}</code></p>',
                unsafe_allow_html=True,
            )
            col3.markdown(
                f'<p style="color:{T["muted"]};font-size:0.8rem;margin:0">SEVERITY</p>'
                f'<p style="margin:0">'
                f'{_severity_span(sev) if sev != "unknown" else "—"}</p>',
                unsafe_allow_html=True,
            )

            if job.get("exc_message"):
                st.error(f"**Error:** {job.get('exc_message', '')}")

            if fa:
                st.markdown(
                    f'<p style="color:{T["text"]}"><strong>Analysis:</strong> '
                    f'{fa.get("explanation", "")}</p>',
                    unsafe_allow_html=True,
                )
                suggestions = fa.get("suggestions", [])
                if suggestions:
                    with st.expander("💡 Suggested Corrective Actions"):
                        for s in suggestions:
                            st.markdown(f"- {s}")

            st.markdown("---")
            can_retry = (
                status in ("failed", "retrying") and
                retry_count < MAX_RETRIES and
                fa.get("is_retryable", True) and
                r is not None
            )

            col_btn, col_info = st.columns([1, 3])
            with col_btn:
                if can_retry:
                    backoff = calculate_backoff(retry_count + 1)
                    if st.button(
                        f"🔁 Retry (backoff: {backoff:.0f}s)",
                        key=f"retry_{job_id}",
                        type="primary",
                    ):
                        with st.spinner("Scheduling retry…"):
                            result = retry_job(r, job_id, triggered_by="manual")
                        if result["success"]:
                            st.success(result["message"])
                            st.rerun()
                        else:
                            st.error(result["message"])
                elif status == "dead":
                    st.markdown(
                        f'<p style="color:{T["muted"]}">💀 <strong>Dead-letter</strong> — max retries exceeded</p>',
                        unsafe_allow_html=True,
                    )
                elif not fa.get("is_retryable", True):
                    st.markdown(
                        f'<p style="color:{T["muted"]}">🚫 Not retryable — fix input first</p>',
                        unsafe_allow_html=True,
                    )
                elif retry_count >= MAX_RETRIES:
                    st.markdown(
                        f'<p style="color:{T["muted"]}">🔒 Max retries ({MAX_RETRIES}) reached</p>',
                        unsafe_allow_html=True,
                    )
                elif r is None:
                    st.markdown(
                        f'<p style="color:{T["muted"]}">⚠️ Redis offline</p>',
                        unsafe_allow_html=True,
                    )

            with col_info:
                if r:
                    attempts = load_job_attempts(r, job_id)
                    if attempts:
                        with st.expander("📜 Attempt History"):
                            for att in attempts:
                                if isinstance(att, str):
                                    try:
                                        att = json.loads(att)
                                    except Exception:
                                        continue
                                st.markdown(
                                    f"**Attempt #{att.get('attempt_index', '?')}** — "
                                    f"triggered by `{att.get('triggered_by', '?')}` — "
                                    f"backoff: {att.get('backoff_s', 0):.0f}s — "
                                    f"status: `{att.get('status', '?')}`"
                                )


# ──────────────────────────────────────────────────────────────────────────────
# Tab 5: Analytics
# ──────────────────────────────────────────────────────────────────────────────

def render_analytics(stats: dict) -> None:
    T = _T()
    st.markdown(
        f'<h3 style="color:{T["text_strong"]}">📊 Analytics & Visualizations</h3>',
        unsafe_allow_html=True,
    )
    st.caption("Charts built from real Redis/RQ data + persisted job history.")

    jobs = list_recent_jobs(limit=200)

    if not jobs:
        st.markdown(
            '<div class="qg-empty">'
            "<h3>📊 No data yet</h3>"
            "<p>Submit some jobs first to populate the analytics dashboard.</p>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    col_chart1, col_chart2 = st.columns(2)

    # ── Status distribution pie ────────────────────────────────────────────
    with col_chart1:
        st.markdown(
            f'<h4 style="color:{T["text_strong"]}">Job Status Distribution</h4>',
            unsafe_allow_html=True,
        )
        status_counts: dict = {}
        for job in jobs:
            s = job.get("status", "unknown")
            status_counts[s] = status_counts.get(s, 0) + 1

        if status_counts:
            # Status colors work on both themes (saturated, distinct hues)
            status_colors = {
                "queued":   "#f59e0b",
                "started":  "#818cf8",
                "finished": "#4ade80",
                "failed":   "#f87171",
                "retrying": "#fb923c",
                "dead":     "#94a3b8",
            }
            fig_pie = px.pie(
                values=list(status_counts.values()),
                names=list(status_counts.keys()),
                color=list(status_counts.keys()),
                color_discrete_map=status_colors,
                hole=0.4,
            )
            fig_pie.update_layout(**chart_layout({"showlegend": True, "margin": dict(t=20, b=20, l=10, r=10)}))
            st.plotly_chart(fig_pie, use_container_width=True)

    # ── Task type bar chart ────────────────────────────────────────────────
    with col_chart2:
        st.markdown(
            f'<h4 style="color:{T["text_strong"]}">Jobs by Task Type</h4>',
            unsafe_allow_html=True,
        )
        task_counts: dict = {}
        for job in jobs:
            t = job.get("task_name", "unknown")
            task_counts[t] = task_counts.get(t, 0) + 1

        if task_counts:
            fig_bar = px.bar(
                x=list(task_counts.keys()),
                y=list(task_counts.values()),
                color=list(task_counts.keys()),
                labels={"x": "Task", "y": "Count"},
                color_discrete_sequence=["#6366f1", "#8b5cf6", "#06b6d4", "#4ade80"],
            )
            fig_bar.update_layout(**chart_layout({"showlegend": False}))
            st.plotly_chart(fig_bar, use_container_width=True)

    # ── Execution timeline ─────────────────────────────────────────────────
    st.markdown(
        f'<h4 style="color:{T["text_strong"]}">Execution Timeline</h4>',
        unsafe_allow_html=True,
    )
    timeline_data = []
    for job in jobs:
        if job.get("start_time") and job.get("finish_time"):
            try:
                start  = datetime.fromisoformat(job["start_time"])
                finish = datetime.fromisoformat(job["finish_time"])
                timeline_data.append({
                    "Task":        job.get("task_name", "?").replace("_", " "),
                    "Job ID":      job.get("job_id", "?")[:10],
                    "Start":       start,
                    "Finish":      finish,
                    "Status":      job.get("status", "?"),
                    "Duration (s)": round((finish - start).total_seconds(), 3),
                })
            except Exception:
                pass

    if timeline_data:
        df_timeline = pd.DataFrame(timeline_data)
        fig_tl = px.timeline(
            df_timeline,
            x_start="Start",
            x_end="Finish",
            y="Task",
            color="Status",
            hover_data=["Job ID", "Duration (s)"],
            color_discrete_map={
                "finished": "#4ade80",
                "failed":   "#f87171",
                "retrying": "#fb923c",
                "dead":     "#94a3b8",
            },
        )
        fig_tl.update_layout(**chart_layout())
        st.plotly_chart(fig_tl, use_container_width=True)
    else:
        st.info("No completed jobs with timing data yet.")

    # ── Failure category breakdown ─────────────────────────────────────────
    failed_with_analysis = [
        j for j in jobs
        if j.get("failure_analysis") and j.get("status") in ("failed", "dead", "retrying")
    ]
    if failed_with_analysis:
        st.markdown(
            f'<h4 style="color:{T["text_strong"]}">Failure Category Breakdown</h4>',
            unsafe_allow_html=True,
        )
        cat_counts: dict = {}
        for j in failed_with_analysis:
            fa = j["failure_analysis"]
            if isinstance(fa, str):
                try:
                    fa = json.loads(fa)
                except Exception:
                    continue
            cat = fa.get("category", "unknown") if isinstance(fa, dict) else "unknown"
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

        if cat_counts:
            fig_cat = px.bar(
                x=list(cat_counts.keys()),
                y=list(cat_counts.values()),
                color=list(cat_counts.keys()),
                labels={"x": "Failure Category", "y": "Count"},
                color_discrete_sequence=["#f87171", "#fb923c", "#fbbf24", "#94a3b8", "#818cf8"],
            )
            fig_cat.update_layout(**chart_layout({"showlegend": False}))
            st.plotly_chart(fig_cat, use_container_width=True)

    # ── Summary stats table ────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        f'<h4 style="color:{T["text_strong"]}">Summary Statistics</h4>',
        unsafe_allow_html=True,
    )
    total    = len(jobs)
    finished = sum(1 for j in jobs if j.get("status") == "finished")
    failed_n = sum(1 for j in jobs if j.get("status") in ("failed", "dead"))
    retrying = sum(1 for j in jobs if j.get("status") == "retrying")
    durations = [j["duration_s"] for j in jobs if j.get("duration_s") is not None]
    avg_dur = sum(durations) / len(durations) if durations else None

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("Total Jobs", total)
    sc2.metric("Success Rate", f"{finished/total*100:.1f}%" if total else "N/A")
    sc3.metric("Failure Rate", f"{failed_n/total*100:.1f}%" if total else "N/A")
    sc4.metric("Avg Duration", _fmt_duration(avg_dur))


# ──────────────────────────────────────────────────────────────────────────────
# Tab 6: System Health
# ──────────────────────────────────────────────────────────────────────────────

def render_system_health(health: dict) -> None:
    T = _T()
    st.markdown(
        f'<h3 style="color:{T["text_strong"]}">🏥 System Health</h3>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f'<h4 style="color:{T["text_strong"]}">Redis Connection</h4>',
            unsafe_allow_html=True,
        )
        if health["connected"]:
            st.success("✅ Connected")
            st.json({
                "host":       health["host"],
                "port":       health["port"],
                "latency_ms": health.get("latency_ms"),
                "status":     "healthy",
            })
        else:
            st.error("❌ Disconnected")
            st.json({"error": health.get("error"), "host": health["host"], "port": health["port"]})

    with col2:
        st.markdown(
            f'<h4 style="color:{T["text_strong"]}">Active Workers</h4>',
            unsafe_allow_html=True,
        )
        if health["connected"]:
            try:
                # BUG FIX: Worker.all() needs the raw bytes client (decode_responses=False)
                # Using the decoded client caused:
                #   AttributeError: 'str' object has no attribute 'decode'
                from queueguard.redis_connection import get_rq_redis_client
                from rq import Worker
                r_raw = get_rq_redis_client()
                workers = Worker.all(connection=r_raw)
                if workers:
                    for w in workers:
                        with st.container():
                            st.success(f"✅ Worker: `{w.name}`")
                            st.caption(
                                f"State: {w.state} | "
                                f"Queues: {', '.join(q.name for q in w.queues)}"
                            )
                else:
                    st.warning("⚠️ No active workers detected.")
                    st.markdown(
                        f'<p style="color:{T["muted"]}">Start a worker in a new terminal:</p>',
                        unsafe_allow_html=True,
                    )
                    st.code("python scripts/start_worker.py", language="powershell")
            except Exception as exc:
                st.error(f"Could not fetch worker info: {exc}")
        else:
            st.warning("Redis offline — cannot check workers.")

    # ── Scheduler status ────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        f'<h4 style="color:{T["text_strong"]}">RQ Scheduler</h4>',
        unsafe_allow_html=True,
    )
    try:
        import rq_scheduler  # noqa: F401
        scheduler_available = True
    except ImportError:
        scheduler_available = False

    if scheduler_available:
        st.info(
            "✅ `rq-scheduler` is installed. For delayed retries (exponential backoff), "
            "run: `python scripts/start_scheduler.py`"
        )
    else:
        st.warning(
            "⚠️ `rq-scheduler` not installed. Retries will be enqueued immediately "
            "(no backoff delay). Install with: `pip install rq-scheduler`"
        )

    # ── Configuration ────────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        f'<h4 style="color:{T["text_strong"]}">Configuration</h4>',
        unsafe_allow_html=True,
    )
    st.json({
        "queue_name":    QUEUE_NAME,
        "max_retries":   MAX_RETRIES,
        "allowed_tasks": ALLOWED_TASKS,
    })

    # ── Maintenance ──────────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        f'<h4 style="color:{T["text_strong"]}">🗑️ Maintenance</h4>',
        unsafe_allow_html=True,
    )
    with st.expander("⚠️ Danger Zone"):
        st.warning("This will permanently delete all job history from Redis.")
        if st.button("🗑️ Clear All History", type="secondary", key="clear_history"):
            r = _get_redis_client_safe()
            if r:
                count = clear_all_history(r)
                st.success(f"Deleted {count} job records.")
                st.rerun()
            else:
                st.error("Redis is offline.")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    # Inject theme CSS first (before any visible content)
    inject_css()

    T = _T()

    # Header
    st.markdown(
        f'<h1 class="qg-header">🛡️ QueueGuard AI</h1>'
        f'<p class="qg-subtitle">AI-Powered Background Job Monitoring & Recovery System</p>',
        unsafe_allow_html=True,
    )

    # Fetch live health + stats once per render
    health = check_redis_health()
    stats: dict = {}
    if health["connected"]:
        try:
            stats = get_queue_stats()
        except Exception:
            stats = {}

    # Sidebar (includes theme selector)
    render_sidebar(health, stats)

    # Redis guard banner
    redis_ok = render_redis_status_banner(health)

    # Main tabs
    tabs = st.tabs([
        "📈 Overview",
        "➕ Submit Job",
        "📋 Job Monitor",
        "❌ Failed Jobs",
        "📊 Analytics",
        "🏥 System Health",
    ])
    tab_overview, tab_submit, tab_monitor, tab_failed, tab_analytics, tab_health = tabs

    with tab_overview:
        if redis_ok:
            render_overview(stats)
        else:
            st.info("Connect Redis to view real-time metrics.")

    with tab_submit:
        if redis_ok:
            render_submit_job()
        else:
            st.warning("Redis must be connected to submit jobs.")

    with tab_monitor:
        if redis_ok:
            render_job_monitor()
        else:
            st.info("Connect Redis to monitor jobs.")

    with tab_failed:
        if redis_ok:
            render_failed_jobs()
        else:
            st.info("Connect Redis to view failed jobs.")

    with tab_analytics:
        if redis_ok:
            render_analytics(stats)
        else:
            st.info("Connect Redis to view analytics.")

    with tab_health:
        render_system_health(health)


if __name__ == "__main__":
    main()
