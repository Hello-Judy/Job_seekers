"""
JobSeekers — Streamlit Dashboard (Handshake-inspired UI polish)
Run from project root:  streamlit run analysis/app.py

UI redesign notes:
    - All business logic, function signatures, and callbacks are UNCHANGED.
    - Only visual layer (CSS, layout primitives, chart styling) is updated.
    - Brand kept: "JobSeekers + 🎯".
    - Style: Handshake-inspired — student-friendly, modern, light-mode,
      generous whitespace, soft shadows, rounded cards, accent purple.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from analysis.db.user_db import (
    init_db, authenticate, register_user,
    get_preferences, save_preferences,
    save_job, get_saved_jobs,
    unsave_job, get_saved_job_ids,
)
from analysis.db.snowflake_reader import (
    get_filter_options, get_cities_for_states,
    get_summary_stats, get_posting_trend,
    get_latest_jobs, search_jobs,
)

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="JobSeekers",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Design tokens — Handshake-inspired light theme
# ─────────────────────────────────────────────────────────────────────────────
# Handshake's signature is white background + bold accent purple, with
# generous whitespace and soft cards. We adopt the same palette.

BRAND_PRIMARY      = "#7B61FF"   # signature accent purple
BRAND_PRIMARY_DARK = "#5B3FD9"
BRAND_SOFT_BG      = "#F4F1FE"   # very pale purple for hover/highlight
TEXT_PRIMARY       = "#1A1A2E"
TEXT_SECONDARY     = "#5C5C7A"
TEXT_MUTED         = "#9999B0"
BORDER_LIGHT       = "#EAEAF2"
SURFACE            = "#FFFFFF"
SURFACE_ALT        = "#FAFAFC"

# Sector colors — used everywhere federal vs private appears.
# Deep blue for federal (authority), warm coral for private (vibrant).
SECTOR_COLOR = {
    "federal": "#2563EB",   # tailwind blue-600
    "private": "#F97316",   # tailwind orange-500
}
SECTOR_LABEL = {
    "federal": "Federal",
    "private": "Private",
}

# Experience level — maps to semantic "growth" colors.
LEVEL_COLOR = {
    "entry":   "#10B981",   # emerald
    "mid":     "#8B5CF6",   # violet
    "senior":  "#EF4444",   # red
    "unknown": "#94A3B8",
}
LEVEL_LABEL = {
    "entry":   "Entry",
    "mid":     "Mid",
    "senior":  "Senior",
    "unknown": "Unknown",
}

REMOTE_ICON = {
    "remote":  "🌐",
    "hybrid":  "🏠",
    "onsite":  "🏢",
    "unknown": "📍",
}

# Plotly color palette derived from the same tokens for chart consistency.
PLOTLY_TEMPLATE = "plotly_white"


# ─────────────────────────────────────────────────────────────────────────────
# Global CSS injection
# ─────────────────────────────────────────────────────────────────────────────
def inject_global_css() -> None:
    """One-shot CSS that restyles Streamlit defaults to match Handshake look."""
    st.markdown(f"""
    <style>
        /* ---- Base typography ------------------------------------------------ */
        html, body, [class*="css"] {{
            font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI',
                         Roboto, 'Helvetica Neue', sans-serif;
            color: {TEXT_PRIMARY};
        }}
        .main {{
            background-color: {SURFACE_ALT};
        }}
        .block-container {{
            padding-top: 2rem;
            padding-bottom: 4rem;
            max-width: 1280px;
        }}
        h1, h2, h3, h4 {{
            color: {TEXT_PRIMARY};
            font-weight: 700;
            letter-spacing: -0.01em;
        }}
        h1 {{ font-size: 2rem; }}
        h2 {{ font-size: 1.5rem; }}
        h3 {{ font-size: 1.15rem; }}
        p, span, div {{
            color: {TEXT_PRIMARY};
        }}
        .stCaption, .caption, [data-testid="stCaptionContainer"] {{
            color: {TEXT_SECONDARY} !important;
        }}

        /* ---- Sidebar -------------------------------------------------------- */
        [data-testid="stSidebar"] {{
            background-color: {SURFACE};
            border-right: 1px solid {BORDER_LIGHT};
        }}
        [data-testid="stSidebar"] .stButton button {{
            background-color: transparent;
            color: {TEXT_PRIMARY};
            border: none;
            text-align: left;
            justify-content: flex-start;
            padding: 0.6rem 0.9rem;
            font-weight: 500;
            border-radius: 8px;
            transition: background-color 0.15s ease;
        }}
        [data-testid="stSidebar"] .stButton button:hover {{
            background-color: {BRAND_SOFT_BG};
            color: {BRAND_PRIMARY_DARK};
        }}
        [data-testid="stSidebar"] .stButton button[kind="primary"] {{
            background-color: {BRAND_SOFT_BG};
            color: {BRAND_PRIMARY_DARK};
            font-weight: 600;
            box-shadow: inset 3px 0 0 {BRAND_PRIMARY};
        }}

        /* ---- Buttons (main area) ------------------------------------------- */
        .main .stButton button[kind="primary"],
        .main .stFormSubmitButton button[kind="primary"],
        .main .stLinkButton a[kind="primary"] {{
            background-color: {BRAND_PRIMARY};
            color: white;
            border: none;
            font-weight: 600;
            border-radius: 8px;
            padding: 0.5rem 1.2rem;
            transition: all 0.15s ease;
            box-shadow: 0 1px 2px rgba(123, 97, 255, 0.15);
        }}
        .main .stButton button[kind="primary"]:hover,
        .main .stFormSubmitButton button[kind="primary"]:hover,
        .main .stLinkButton a[kind="primary"]:hover {{
            background-color: {BRAND_PRIMARY_DARK};
            box-shadow: 0 4px 12px rgba(123, 97, 255, 0.25);
            transform: translateY(-1px);
        }}
        .main .stButton button[kind="secondary"],
        .main .stLinkButton a:not([kind="primary"]) {{
            background-color: {SURFACE};
            color: {TEXT_PRIMARY};
            border: 1px solid {BORDER_LIGHT};
            font-weight: 500;
            border-radius: 8px;
        }}
        .main .stButton button[kind="secondary"]:hover {{
            border-color: {BRAND_PRIMARY};
            color: {BRAND_PRIMARY_DARK};
        }}

        /* ---- Cards / containers -------------------------------------------- */
        [data-testid="stVerticalBlockBorderWrapper"] {{
            background-color: {SURFACE};
            border: 1px solid {BORDER_LIGHT} !important;
            border-radius: 12px !important;
            padding: 1rem 1.2rem !important;
            box-shadow: 0 1px 3px rgba(20, 20, 50, 0.04);
            transition: box-shadow 0.18s ease, border-color 0.18s ease;
        }}
        [data-testid="stVerticalBlockBorderWrapper"]:hover {{
            border-color: {BRAND_PRIMARY} !important;
            box-shadow: 0 6px 16px rgba(123, 97, 255, 0.10);
        }}

        /* ---- Metric cards (KPI tiles) -------------------------------------- */
        [data-testid="stMetric"] {{
            background-color: {SURFACE};
            border: 1px solid {BORDER_LIGHT};
            border-radius: 12px;
            padding: 1.1rem 1.2rem;
            box-shadow: 0 1px 3px rgba(20, 20, 50, 0.04);
        }}
        [data-testid="stMetricLabel"] {{
            color: {TEXT_SECONDARY} !important;
            font-size: 0.78rem !important;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }}
        [data-testid="stMetricValue"] {{
            color: {TEXT_PRIMARY} !important;
            font-size: 1.85rem !important;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }}

        /* ---- Inputs --------------------------------------------------------- */
        .stTextInput input, .stSelectbox > div > div, .stMultiSelect > div > div,
        .stDateInput input {{
            border-radius: 8px !important;
            border-color: {BORDER_LIGHT} !important;
        }}
        .stTextInput input:focus, .stDateInput input:focus {{
            border-color: {BRAND_PRIMARY} !important;
            box-shadow: 0 0 0 3px rgba(123, 97, 255, 0.12) !important;
        }}

        /* ---- Tabs ----------------------------------------------------------- */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 4px;
            border-bottom: 1px solid {BORDER_LIGHT};
        }}
        .stTabs [data-baseweb="tab"] {{
            background-color: transparent;
            border-radius: 8px 8px 0 0;
            color: {TEXT_SECONDARY};
            font-weight: 500;
        }}
        .stTabs [aria-selected="true"] {{
            color: {BRAND_PRIMARY_DARK} !important;
            background-color: {BRAND_SOFT_BG} !important;
            border-bottom: 2px solid {BRAND_PRIMARY} !important;
        }}

        /* ---- Alerts (info / warning / success) ----------------------------- */
        [data-testid="stAlert"] {{
            border-radius: 10px;
            border: none;
            padding: 0.9rem 1.1rem;
        }}

        /* ---- Hide Streamlit default chrome --------------------------------- */
        #MainMenu, footer, header {{visibility: hidden;}}
        [data-testid="stToolbar"] {{visibility: hidden;}}

        /* ---- Custom helper classes ----------------------------------------- */
        .js-pill {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.02em;
            line-height: 1.4;
        }}
        .js-pill-soft {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.02em;
            line-height: 1.4;
        }}
        .js-job-title {{
            font-size: 1.05rem;
            font-weight: 700;
            color: {TEXT_PRIMARY};
            margin: 0 0 4px 0;
            line-height: 1.35;
        }}
        .js-job-company {{
            font-size: 0.9rem;
            color: {TEXT_SECONDARY};
            font-weight: 500;
        }}
        .js-job-meta {{
            font-size: 0.82rem;
            color: {TEXT_SECONDARY};
            margin-top: 8px;
        }}
        .js-salary {{
            font-size: 0.95rem;
            font-weight: 700;
            color: {TEXT_PRIMARY};
            font-variant-numeric: tabular-nums;
        }}
        .js-divider-soft {{
            border: none;
            border-top: 1px solid {BORDER_LIGHT};
            margin: 12px 0 10px 0;
        }}
        .js-hero {{
            background: linear-gradient(135deg, {BRAND_PRIMARY} 0%, {BRAND_PRIMARY_DARK} 100%);
            color: white;
            padding: 1.8rem 2rem;
            border-radius: 16px;
            margin-bottom: 1.8rem;
            box-shadow: 0 8px 24px rgba(123, 97, 255, 0.20);
        }}
        .js-hero h1 {{
            color: white;
            font-size: 1.8rem;
            margin: 0 0 6px 0;
        }}
        .js-hero p {{
            color: rgba(255, 255, 255, 0.85);
            margin: 0;
            font-size: 0.95rem;
        }}
        .js-section-header {{
            display: flex;
            align-items: center;
            gap: 8px;
            margin: 1.2rem 0 0.8rem 0;
        }}
        .js-section-header h3 {{
            margin: 0;
            font-size: 1.15rem;
            font-weight: 700;
        }}
        .js-section-header .js-tag {{
            background: {BRAND_SOFT_BG};
            color: {BRAND_PRIMARY_DARK};
            padding: 2px 8px;
            border-radius: 6px;
            font-size: 0.72rem;
            font-weight: 600;
        }}
        .js-realtime-badge {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            background: #ECFDF5;
            color: #047857;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        .js-realtime-dot {{
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: #10B981;
            box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.25);
            animation: js-pulse 2s ease-in-out infinite;
        }}
        @keyframes js-pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.4; }}
        }}
        .js-auth-shell {{
            text-align: center;
            padding: 2rem 0 1.5rem 0;
        }}
        .js-brand-mark {{
            font-size: 2.2rem;
            margin-bottom: 4px;
        }}
        .js-brand-name {{
            font-size: 1.6rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            color: {TEXT_PRIMARY};
            margin: 0;
        }}
        .js-brand-tagline {{
            color: {TEXT_SECONDARY};
            font-size: 0.92rem;
            margin: 4px 0 0 0;
        }}
    </style>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Session defaults
# ─────────────────────────────────────────────────────────────────────────────
_defaults = {"user": None, "user_name": "", "page": "dashboard", "saved_ids": None}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers (unchanged signatures)
# ─────────────────────────────────────────────────────────────────────────────
def _salary(row: dict) -> str:
    lo, hi = row.get("salary_min"), row.get("salary_max")
    if lo and hi:
        return f"${lo:,.0f} – ${hi:,.0f}"
    if lo:
        return f"${lo:,.0f}+"
    if hi:
        return f"up to ${hi:,.0f}"
    return "Salary not listed"


def _loc(row: dict) -> str:
    # Guard against pandas' float-NaN sentinel: filter(None, ...) drops
    # Python None but keeps NaN (it's a truthy float), and str.join then
    # blows up. Coerce every part to str and drop blanks.
    parts = []
    for key in ("location_city", "location_state"):
        v = row.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            parts.append(s)
    return ", ".join(parts) or "Unknown location"


def _ensure_saved_ids() -> None:
    if st.session_state.get("saved_ids") is None and st.session_state.user:
        st.session_state.saved_ids = get_saved_job_ids(st.session_state.user)


def _greet() -> str:
    h = date.today().timetuple().tm_hour
    return "morning" if h < 12 else "afternoon" if h < 18 else "evening"


def _sector_pill_html(sector: str) -> str:
    color = SECTOR_COLOR.get(sector, "#94A3B8")
    label = SECTOR_LABEL.get(sector, sector.title())
    return (
        f"<span class='js-pill' "
        f"style='background:{color}1A;color:{color};border:1px solid {color}33'>"
        f"{label}</span>"
    )


def _level_pill_html(level: str) -> str:
    color = LEVEL_COLOR.get(level, "#94A3B8")
    label = LEVEL_LABEL.get(level, level.title() if level else "—")
    return (
        f"<span class='js-pill-soft' "
        f"style='background:{color}1A;color:{color}'>"
        f"{label}</span>"
    )


def _remote_pill_html(remote: str) -> str:
    icon = REMOTE_ICON.get(remote, "📍")
    label = remote.title() if remote and remote != "unknown" else "Not specified"
    return (
        f"<span class='js-pill-soft' "
        f"style='background:{SURFACE_ALT};color:{TEXT_SECONDARY};"
        f"border:1px solid {BORDER_LIGHT}'>"
        f"{icon} {label}</span>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Job card (functional callbacks UNCHANGED — only HTML/CSS restyled)
# ─────────────────────────────────────────────────────────────────────────────
def job_card(row: dict, key_suffix: str = "") -> None:
    sector  = row.get("sector", "private")
    level   = row.get("experience_level", "unknown")
    remote  = row.get("remote_type", "unknown")
    url     = (row.get("apply_url") or "").strip()
    job_id  = str(row.get("job_id", ""))
    source  = str(row.get("source", ""))

    _ensure_saved_ids()
    is_saved = (job_id, source) in (st.session_state.saved_ids or set())

    with st.container(border=True):
        # ── Header: title + sector pill ─────────────────────────────────────
        h_left, h_right = st.columns([5, 1])
        with h_left:
            st.markdown(
                f"<div class='js-job-title'>{row.get('job_title', 'Untitled')}</div>"
                f"<div class='js-job-company'>{row.get('company_or_agency', 'Unknown')}</div>",
                unsafe_allow_html=True,
            )
        with h_right:
            st.markdown(
                f"<div style='text-align:right'>{_sector_pill_html(sector)}</div>",
                unsafe_allow_html=True,
            )

        # ── Meta row: location + level + remote pills ───────────────────────
        st.markdown(
            f"<div class='js-job-meta'>"
            f"📍 {_loc(row)} &nbsp; "
            f"{_level_pill_html(level)} &nbsp; "
            f"{_remote_pill_html(remote)}"
            f"</div>",
            unsafe_allow_html=True,
        )

        # ── Salary + posted date row ────────────────────────────────────────
        posted = row.get("posted_date", "")
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;"
            f"align-items:center;margin-top:10px'>"
            f"<span class='js-salary'>{_salary(row)}</span>"
            f"<span style='color:{TEXT_MUTED};font-size:0.8rem'>"
            f"Posted {posted}</span>"
            f"</div>"
            f"<hr class='js-divider-soft'/>",
            unsafe_allow_html=True,
        )

        # ── Apply + Save buttons ────────────────────────────────────────────
        btn_key = f"save_{job_id}{key_suffix}"
        col_apply, col_save = st.columns([3, 1])
        with col_apply:
            if url:
                st.link_button("Apply Now →", url, type="primary", use_container_width=True)
            else:
                st.button("No link available", disabled=True,
                          use_container_width=True, key=f"nolink_{btn_key}")
        with col_save:
            if is_saved:
                if st.button("✓ Saved", key=btn_key, use_container_width=True, type="primary"):
                    unsave_job(st.session_state.user, job_id, source)
                    st.session_state.saved_ids.discard((job_id, source))
                    st.toast("Removed from saved jobs.", icon="🗑️")
                    st.rerun()
            else:
                if st.button("☆ Save", key=btn_key, use_container_width=True):
                    save_job(st.session_state.user, row)
                    if st.session_state.saved_ids is not None:
                        st.session_state.saved_ids.add((job_id, source))
                    st.toast("Job saved!", icon="📌")
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Auth page
# ─────────────────────────────────────────────────────────────────────────────
def auth_page() -> None:
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        # Brand block
        st.markdown(
            """
            <div class='js-auth-shell'>
                <div class='js-brand-mark'>🎯</div>
                <p class='js-brand-name'>JobSeekers</p>
                <p class='js-brand-tagline'>
                    Federal &amp; private job market explorer for new graduates
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        tab_in, tab_up = st.tabs(["Sign In", "Create Account"])

        with tab_in:
            with st.form("login"):
                email = st.text_input("Email", placeholder="you@example.com")
                pw    = st.text_input("Password", type="password",
                                       placeholder="Your password")
                ok    = st.form_submit_button("Sign In", type="primary",
                                               use_container_width=True)
            if ok:
                user = authenticate(email, pw)
                if user:
                    st.session_state.user      = user["email"]
                    st.session_state.user_name = user["name"]
                    st.rerun()
                else:
                    st.error("Invalid email or password.")

        with tab_up:
            with st.form("register"):
                name    = st.text_input("Full Name", placeholder="Jane Doe")
                email_r = st.text_input("Email",
                                         placeholder="you@example.com",
                                         help="Used as your login ID")
                pw_r    = st.text_input("Password", type="password", key="pw_r",
                                         placeholder="At least 6 characters")
                pw_r2   = st.text_input("Confirm Password", type="password",
                                         placeholder="Re-enter password")
                ok_r    = st.form_submit_button("Create Account", type="primary",
                                                 use_container_width=True)
            if ok_r:
                err = None
                if not all([name, email_r, pw_r, pw_r2]):
                    err = "All fields are required."
                elif "@" not in email_r:
                    err = "Please enter a valid email address."
                elif pw_r != pw_r2:
                    err = "Passwords do not match."
                elif len(pw_r) < 6:
                    err = "Password must be at least 6 characters."

                if err:
                    st.error(err)
                elif register_user(email_r, name, pw_r):
                    st.success("Account created! Please sign in.")
                else:
                    st.error("That email is already registered.")


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
def sidebar() -> None:
    with st.sidebar:
        # Brand
        st.markdown(
            f"""
            <div style='padding:0.4rem 0 1rem 0;border-bottom:1px solid {BORDER_LIGHT};
                        margin-bottom:1rem'>
                <div style='font-size:1.4rem;font-weight:800;color:{TEXT_PRIMARY};
                            letter-spacing:-0.02em'>
                    🎯 JobSeekers
                </div>
                <div style='font-size:0.78rem;color:{TEXT_MUTED};margin-top:2px'>
                    Find your next role
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # User info
        initial = (st.session_state.user_name or "?")[0].upper()
        st.markdown(
            f"""
            <div style='display:flex;align-items:center;gap:10px;
                        padding:0.4rem 0 1rem 0'>
                <div style='width:36px;height:36px;border-radius:50%;
                            background:{BRAND_SOFT_BG};color:{BRAND_PRIMARY_DARK};
                            display:flex;align-items:center;justify-content:center;
                            font-weight:700;font-size:0.95rem'>
                    {initial}
                </div>
                <div style='flex:1;min-width:0'>
                    <div style='font-weight:600;color:{TEXT_PRIMARY};font-size:0.92rem'>
                        {st.session_state.user_name}
                    </div>
                    <div style='color:{TEXT_MUTED};font-size:0.76rem;
                                white-space:nowrap;overflow:hidden;text-overflow:ellipsis'>
                        {st.session_state.user}
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Nav
        pages = {
            "dashboard":   "🏠  Dashboard",
            "search":      "🔍  Job Search",
            "analytics":   "📊  Analytics",
            "preferences": "⚙️  Preferences",
            "saved":       "📌  Saved Jobs",
        }
        for key, label in pages.items():
            if st.button(label, use_container_width=True,
                         type="primary" if st.session_state.page == key else "secondary",
                         key=f"nav_{key}"):
                st.session_state.page = key
                st.rerun()

        st.markdown("<div style='flex:1;min-height:1.5rem'></div>", unsafe_allow_html=True)
        st.divider()
        if st.button("← Sign Out", use_container_width=True, key="signout"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.session_state.saved_ids = None
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────────────────────────
def dashboard() -> None:
    first_name = st.session_state.user_name.split()[0] if st.session_state.user_name else "there"

    # ── Hero header ──────────────────────────────────────────────────────────
    st.markdown(
        f"""
        <div class='js-hero'>
            <div style='display:flex;justify-content:space-between;align-items:center;
                        flex-wrap:wrap;gap:12px'>
                <div>
                    <h1>Good {_greet()}, {first_name} 👋</h1>
                    <p>{date.today().strftime('%A, %B %d, %Y')} · Here's what's new in the federal &amp; private job market.</p>
                </div>
                <span class='js-realtime-badge'>
                    <span class='js-realtime-dot'></span> Live · Updates every 30 min
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Summary metrics ──────────────────────────────────────────────────────
    try:
        s = get_summary_stats()
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Jobs",   f"{int(s.get('total_jobs',     0)):,}")
        c2.metric("Federal",      f"{int(s.get('federal_jobs',   0)):,}")
        c3.metric("Private",      f"{int(s.get('private_jobs',   0)):,}")
        c4.metric("Entry Level",  f"{int(s.get('entry_level_jobs',0)):,}")
        c5.metric("New (7 days)", f"{int(s.get('jobs_last_7d',   0)):,}")
    except Exception as exc:
        st.warning(f"Stats unavailable: {exc}")

    st.markdown("<div style='height:1.2rem'></div>", unsafe_allow_html=True)

    # ── Trend chart + live feed ──────────────────────────────────────────────
    col_chart, col_feed = st.columns([3, 2])

    with col_chart:
        st.markdown(
            "<div class='js-section-header'>"
            "<h3>📈 Daily Posting Trend</h3>"
            "<span class='js-tag'>Last 30 days</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        try:
            df = get_posting_trend(30)
            if df.empty:
                st.info("No trend data available yet. Run the pipeline first.")
            else:
                df["day"] = pd.to_datetime(df["day"])
                fig = px.bar(
                    df, x="day", y="job_count", color="sector",
                    color_discrete_map=SECTOR_COLOR,
                    barmode="stack",
                    labels={"day": "", "job_count": "", "sector": ""},
                    template=PLOTLY_TEMPLATE,
                )
                fig.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    legend=dict(orientation="h", y=1.08, x=1, xanchor="right",
                                font=dict(size=11)),
                    margin=dict(l=0, r=0, t=30, b=0),
                    height=320,
                    xaxis=dict(showgrid=False, tickfont=dict(color=TEXT_SECONDARY, size=11)),
                    yaxis=dict(gridcolor=BORDER_LIGHT,
                                tickfont=dict(color=TEXT_SECONDARY, size=11)),
                    font=dict(family="Inter, sans-serif"),
                )
                fig.update_traces(marker_line_width=0)
                st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:
            st.warning(f"Trend chart unavailable: {exc}")

    with col_feed:
        st.markdown(
            "<div class='js-section-header'>"
            "<h3>🔔 Latest Postings</h3>"
            "<span class='js-tag'>Personalized</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        prefs = get_preferences(st.session_state.user)
        try:
            df_feed = get_latest_jobs(
                experience_levels=tuple(prefs.get("experience_level", [])),
                sectors=tuple(prefs.get("sector", [])),
                location_states=tuple(prefs.get("location_state", [])),
                remote_types=tuple(prefs.get("remote_type", [])),
                days=7, limit=8,
            )
            if df_feed.empty:
                st.info("No recent jobs match your preferences.\n\n"
                        "Go to **⚙️ Preferences** to set up your feed.")
            else:
                for _, row in df_feed.iterrows():
                    rd = row.to_dict()
                    sector = rd.get("sector", "private")
                    with st.container(border=True):
                        left, right = st.columns([3, 1])
                        with left:
                            st.markdown(
                                f"<div style='font-weight:700;color:{TEXT_PRIMARY};"
                                f"font-size:0.95rem'>{rd.get('job_title','Untitled')}</div>"
                                f"<div style='color:{TEXT_SECONDARY};font-size:0.82rem;"
                                f"margin-top:2px'>"
                                f"{rd.get('company_or_agency','')} · {_loc(rd)}"
                                f"</div>"
                                f"<div style='margin-top:6px'>"
                                f"{_sector_pill_html(sector)} &nbsp; "
                                f"{_level_pill_html(rd.get('experience_level','unknown'))}"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                        with right:
                            url = (rd.get("apply_url") or "").strip()
                            if url:
                                st.link_button("Apply", url, use_container_width=True)
        except Exception as exc:
            st.warning(f"Feed unavailable: {exc}")

    # ── Recommended grid ─────────────────────────────────────────────────────
    st.markdown("<div style='height:1.2rem'></div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='js-section-header'>"
        "<h3>🎯 Recommended for You</h3>"
        "<span class='js-tag'>Based on your preferences</span>"
        "</div>",
        unsafe_allow_html=True,
    )
    prefs = get_preferences(st.session_state.user)
    has_prefs = any(prefs.get(k) for k in ("experience_level", "sector", "location_state"))
    if not has_prefs:
        st.info("Set your preferences in **⚙️ Preferences** to see personalized recommendations.")
    else:
        try:
            df_rec = get_latest_jobs(
                experience_levels=tuple(prefs.get("experience_level", [])),
                sectors=tuple(prefs.get("sector", [])),
                location_states=tuple(prefs.get("location_state", [])),
                days=14, limit=6,
            )
            if df_rec.empty:
                st.info("No jobs match your preferences right now — check back after the next pipeline run.")
            else:
                cols = st.columns(3)
                for i, (_, row) in enumerate(df_rec.iterrows()):
                    with cols[i % 3]:
                        job_card(row.to_dict(), key_suffix=f"_rec{i}")
        except Exception as exc:
            st.warning(f"Recommendations unavailable: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Job Search
# ─────────────────────────────────────────────────────────────────────────────
def job_search() -> None:
    st.markdown(
        f"<h1 style='margin-bottom:0.2rem'>🔍 Find Your Next Role</h1>"
        f"<p style='color:{TEXT_SECONDARY};margin-bottom:1.5rem'>"
        f"Filter across federal and private listings."
        f"</p>",
        unsafe_allow_html=True,
    )

    try:
        opts = get_filter_options()
        all_states     = opts.get("states", [])
        all_categories = opts.get("categories", [])
    except Exception:
        all_states, all_categories = [], []

    # ── filters (left column) ────────────────────────────────────────────────
    fcol, rcol = st.columns([1, 3])

    with fcol:
        with st.container(border=True):
            st.markdown(
                f"<div style='font-weight:700;color:{TEXT_PRIMARY};font-size:1rem;"
                f"margin-bottom:0.6rem'>Filters</div>",
                unsafe_allow_html=True,
            )

            st.caption("📅 Posted Date")
            d_from = st.date_input("From", value=date.today() - timedelta(days=30), key="d_from")
            d_to   = st.date_input("To",   value=date.today(),                       key="d_to")

            st.caption("📍 Location")
            sel_states = st.multiselect("State(s)", options=all_states, key="sel_states")
            city_opts = []
            if sel_states:
                try:
                    city_opts = get_cities_for_states(tuple(sel_states))
                except Exception:
                    pass
            sel_cities = st.multiselect("City / Cities", options=city_opts,
                                        disabled=not sel_states, key="sel_cities")

            st.caption("💼 Job Category")
            sel_cats = st.multiselect("Category", options=all_categories, key="sel_cats")

            st.caption("🎓 Experience Level")
            sel_levels = st.multiselect(
                "Level",
                options=["entry", "mid", "senior"],
                format_func=lambda x: LEVEL_LABEL.get(x, x),
                key="sel_levels",
            )

            st.caption("🏛 Sector")
            sel_sectors = st.multiselect(
                "Sector",
                options=["federal", "private"],
                format_func=lambda x: SECTOR_LABEL.get(x, x.title()),
                key="sel_sectors",
            )

            st.caption("💻 Work Mode")
            sel_remote = st.multiselect(
                "Mode",
                options=["remote", "hybrid", "onsite", "unknown"],
                format_func=lambda x: {
                    "remote": "🌐 Remote", "hybrid": "🏠 Hybrid",
                    "onsite": "🏢 On-site", "unknown": "❓ Not specified",
                }.get(x, x),
                key="sel_remote",
            )

            search_btn = st.button("🔍 Search Jobs", type="primary", use_container_width=True)

    # ── results (right column) ───────────────────────────────────────────────
    with rcol:
        if search_btn:
            try:
                df = search_jobs(
                    states=sel_states,
                    cities=sel_cities,
                    categories=sel_cats,
                    experience_levels=sel_levels,
                    sectors=sel_sectors,
                    remote_types=sel_remote,
                    date_from=str(d_from) if d_from else None,
                    date_to=str(d_to)   if d_to   else None,
                )
                st.session_state["search_df"] = df
            except Exception as exc:
                st.error(f"Search failed: {exc}")
                return

        df = st.session_state.get("search_df", pd.DataFrame())

        if df.empty:
            if search_btn:
                st.info("No jobs match your filters. Try broadening the search.")
            else:
                st.info("Use the filters on the left and click **Search Jobs**.")
            return

        # Result header & sort
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;"
            f"align-items:center;margin-bottom:0.8rem'>"
            f"<div style='font-size:1.05rem;font-weight:700;color:{TEXT_PRIMARY}'>"
            f"{len(df)} job{'s' if len(df) != 1 else ''} found"
            f"</div></div>",
            unsafe_allow_html=True,
        )
        sort_by = st.selectbox(
            "Sort by",
            ["Newest", "Oldest", "Salary ↓", "Salary ↑"],
        )

        if sort_by == "Oldest":
            df = df.sort_values("posted_date", ascending=True)
        elif sort_by == "Salary ↓":
            df = df.sort_values("salary_max", ascending=False, na_position="last")
        elif sort_by == "Salary ↑":
            df = df.sort_values("salary_min", ascending=True, na_position="last")

        for i, (_, row) in enumerate(df.iterrows()):
            job_card(row.to_dict(), key_suffix=f"_s{i}")


# ─────────────────────────────────────────────────────────────────────────────
# Preferences
# ─────────────────────────────────────────────────────────────────────────────
def preferences() -> None:
    st.markdown(
        f"<h1 style='margin-bottom:0.2rem'>⚙️ Your Preferences</h1>"
        f"<p style='color:{TEXT_SECONDARY};margin-bottom:1.5rem'>"
        f"Tell us what you're looking for. We'll personalize your Dashboard feed."
        f"</p>",
        unsafe_allow_html=True,
    )

    prefs = get_preferences(st.session_state.user)
    try:
        opts = get_filter_options()
        all_states = opts.get("states", [])
    except Exception:
        all_states = []

    with st.container(border=True):
        with st.form("pref_form"):
            st.markdown(f"<div style='font-weight:700;font-size:1rem;"
                        f"color:{TEXT_PRIMARY};margin-bottom:0.4rem'>"
                        f"🎓 Experience Level</div>", unsafe_allow_html=True)
            sel_levels = st.multiselect(
                "I want to see:",
                options=["entry", "mid", "senior"],
                default=prefs.get("experience_level", []),
                format_func=lambda x: LEVEL_LABEL.get(x, x),
            )

            st.markdown(f"<div style='font-weight:700;font-size:1rem;"
                        f"color:{TEXT_PRIMARY};margin:1rem 0 0.4rem 0'>"
                        f"🏛 Sector</div>", unsafe_allow_html=True)
            sel_sectors = st.multiselect(
                "Track postings from:",
                options=["federal", "private"],
                default=prefs.get("sector", []),
                format_func=lambda x: SECTOR_LABEL.get(x, x.title()),
            )

            st.markdown(f"<div style='font-weight:700;font-size:1rem;"
                        f"color:{TEXT_PRIMARY};margin:1rem 0 0.4rem 0'>"
                        f"📍 Location</div>", unsafe_allow_html=True)
            sel_states = st.multiselect(
                "Focus on:",
                options=all_states,
                default=[s for s in prefs.get("location_state", []) if s in all_states],
            )

            st.markdown(f"<div style='font-weight:700;font-size:1rem;"
                        f"color:{TEXT_PRIMARY};margin:1rem 0 0.4rem 0'>"
                        f"💻 Work Mode</div>", unsafe_allow_html=True)
            sel_remote = st.multiselect(
                "Mode:",
                options=["remote", "hybrid", "onsite"],
                default=prefs.get("remote_type", []),
                format_func=lambda x: {"remote": "🌐 Remote",
                                         "hybrid": "🏠 Hybrid",
                                         "onsite": "🏢 On-site"}.get(x, x),
            )

            st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
            saved = st.form_submit_button("💾 Save Preferences", type="primary",
                                            use_container_width=True)

    if saved:
        save_preferences(st.session_state.user, {
            "experience_level": sel_levels,
            "sector":           sel_sectors,
            "location_state":   sel_states,
            "remote_type":      sel_remote,
        })
        st.success("Preferences saved! Your Dashboard feed will update on next reload.")
        st.cache_data.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Saved Jobs
# ─────────────────────────────────────────────────────────────────────────────
def saved_jobs() -> None:
    st.markdown(
        f"<h1 style='margin-bottom:0.2rem'>📌 Saved Jobs</h1>",
        unsafe_allow_html=True,
    )

    jobs = get_saved_jobs(st.session_state.user)

    if not jobs:
        st.markdown(
            f"<p style='color:{TEXT_SECONDARY};margin-bottom:1.5rem'>"
            f"Your bookmarked roles will appear here."
            f"</p>",
            unsafe_allow_html=True,
        )
        st.info("You haven't saved any jobs yet.\n\n"
                "Use **🔍 Job Search** or the **Dashboard** feed to save jobs you're interested in.")
        return

    st.markdown(
        f"<p style='color:{TEXT_SECONDARY};margin-bottom:1.5rem'>"
        f"{len(jobs)} role{'s' if len(jobs) != 1 else ''} saved"
        f"</p>",
        unsafe_allow_html=True,
    )

    for job in jobs:
        with st.container(border=True):
            left, mid, right = st.columns([4, 1, 1])
            with left:
                st.markdown(
                    f"<div style='font-weight:700;color:{TEXT_PRIMARY};"
                    f"font-size:1rem'>{job.get('job_title','Untitled')}</div>"
                    f"<div style='color:{TEXT_SECONDARY};font-size:0.85rem;"
                    f"margin-top:2px'>"
                    f"{job.get('company','')} · {job.get('location','')}"
                    f"</div>"
                    f"<div style='color:{TEXT_MUTED};font-size:0.78rem;margin-top:6px'>"
                    f"Saved on {str(job.get('saved_at',''))[:10]}"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with mid:
                url = (job.get("apply_url") or "").strip()
                if url:
                    st.link_button("Apply →", url,
                                    use_container_width=True, type="primary")
            with right:
                rm_key = f"rm_{job.get('job_id','')}_{job.get('source','')}"
                if st.button("✕ Remove", key=rm_key, use_container_width=True):
                    unsave_job(
                        st.session_state.user,
                        job.get("job_id", ""),
                        job.get("source", ""),
                    )
                    if st.session_state.saved_ids is not None:
                        st.session_state.saved_ids.discard(
                            (job.get("job_id", ""), job.get("source", ""))
                        )
                    st.toast("Job removed.", icon="🗑️")
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    init_db()
    inject_global_css()

    if not st.session_state.user:
        auth_page()
        return

    sidebar()

    page = st.session_state.page
    if page == "dashboard":
        dashboard()
    elif page == "search":
        job_search()
    elif page == "analytics":
        from analysis.analytics_page import render_analytics_page
        render_analytics_page()
    elif page == "preferences":
        preferences()
    elif page == "saved":
        saved_jobs()


if __name__ == "__main__":
    main()