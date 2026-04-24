"""
JobSeekers — Streamlit Dashboard
Run from project root:  streamlit run analysis/app.py
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
# Session defaults
# ─────────────────────────────────────────────────────────────────────────────
_defaults = {"user": None, "user_name": "", "page": "dashboard"}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────
SECTOR_COLOR = {"federal": "#1565C0", "private": "#E65100"}
LEVEL_COLOR  = {"entry": "#2E7D32", "mid": "#6A1B9A", "senior": "#B71C1C", "unknown": "#546E7A"}
LEVEL_LABEL  = {"entry": "🌱 Entry", "mid": "🌿 Mid / Junior", "senior": "🌳 Senior", "unknown": "❓ Unknown"}
REMOTE_ICON  = {"remote": "🌐", "hybrid": "🏠", "onsite": "🏢", "unknown": "📍"}


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
    return ", ".join(filter(None, [row.get("location_city"), row.get("location_state")])) or "Unknown location"


def job_card(row: dict, key_suffix: str = "") -> None:
    """Render a single job card inside the caller's column/container."""
    sector  = row.get("sector", "private")
    level   = row.get("experience_level", "unknown")
    remote  = row.get("remote_type", "unknown")
    url     = (row.get("apply_url") or "").strip()

    sc = SECTOR_COLOR.get(sector, "#546E7A")
    lc = LEVEL_COLOR.get(level, "#546E7A")

    with st.container(border=True):
        # ── title row ────────────────────────────────────────────────────────
        h_left, h_right = st.columns([4, 1])
        with h_left:
            st.markdown(f"**{row.get('job_title', 'Untitled')}**")
            st.caption(f"🏢 {row.get('company_or_agency', 'Unknown')}")
        with h_right:
            badge = "🏛 Federal" if sector == "federal" else "🏪 Private"
            st.markdown(
                f"<span style='background:{sc};color:#fff;"
                f"padding:2px 7px;border-radius:12px;font-size:.72rem'>{badge}</span>",
                unsafe_allow_html=True,
            )

        # ── detail row ───────────────────────────────────────────────────────
        st.markdown(
            f"{REMOTE_ICON.get(remote,'📍')} {_loc(row)} &nbsp;|&nbsp; "
            f"<span style='color:{lc};font-weight:600'>{LEVEL_LABEL.get(level,level)}</span>",
            unsafe_allow_html=True,
        )
        st.caption(f"💰 {_salary(row)}   📅 {row.get('posted_date','')}")

        # ── apply button ─────────────────────────────────────────────────────
        btn_key = f"save_{row.get('job_id','')}{key_suffix}"
        col_apply, col_save = st.columns([3, 1])
        with col_apply:
            if url:
                st.link_button("Apply Now →", url, type="primary", use_container_width=True)
            else:
                st.button("No link available", disabled=True,
                          use_container_width=True, key=f"nolink_{btn_key}")
        with col_save:
            if st.button("📌 Save", key=btn_key, use_container_width=True):
                save_job(st.session_state.user, row)
                st.toast("Job saved!", icon="📌")


# ─────────────────────────────────────────────────────────────────────────────
# Auth page
# ─────────────────────────────────────────────────────────────────────────────
def auth_page() -> None:
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.title("🎯 JobSeekers")
        st.caption("Federal & private job market explorer for new graduates")
        st.divider()

        tab_in, tab_up = st.tabs(["Sign In", "Create Account"])

        with tab_in:
            with st.form("login"):
                email = st.text_input("Email")
                pw    = st.text_input("Password", type="password")
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
                name  = st.text_input("Full Name")
                email_r = st.text_input("Email  (used as your login ID)")
                pw_r  = st.text_input("Password", type="password", key="pw_r")
                pw_r2 = st.text_input("Confirm Password", type="password")
                ok_r  = st.form_submit_button("Create Account", type="primary",
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
# Sidebar (logged-in)
# ─────────────────────────────────────────────────────────────────────────────
def sidebar() -> None:
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state.user_name}")
        st.caption(st.session_state.user)
        st.divider()

        pages = {
            "dashboard":   "🏠 Dashboard",
            "search":      "🔍 Job Search",
            "preferences": "⚙️  Preferences",
            "saved":       "📌 Saved Jobs",
        }
        for key, label in pages.items():
            if st.button(label, use_container_width=True,
                         type="primary" if st.session_state.page == key else "secondary"):
                st.session_state.page = key
                st.rerun()

        st.divider()
        if st.button("Sign Out", use_container_width=True):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────────────────────────
def dashboard() -> None:
    st.header(f"Good {_greet()}, {st.session_state.user_name.split()[0]}! 👋")
    st.caption(f"Today is {date.today().strftime('%A, %B %d, %Y')}")

    # ── summary metrics ───────────────────────────────────────────────────────
    try:
        s = get_summary_stats()
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Jobs",     f"{int(s.get('total_jobs',     0)):,}")
        c2.metric("Federal",        f"{int(s.get('federal_jobs',   0)):,}")
        c3.metric("Private",        f"{int(s.get('private_jobs',   0)):,}")
        c4.metric("Entry Level",    f"{int(s.get('entry_level_jobs',0)):,}")
        c5.metric("New (7 days)",   f"{int(s.get('jobs_last_7d',   0)):,}")
    except Exception as exc:
        st.warning(f"Stats unavailable: {exc}")

    st.divider()

    # ── trend chart + live feed ───────────────────────────────────────────────
    col_chart, col_feed = st.columns([3, 2])

    with col_chart:
        st.subheader("📈 Daily Posting Trend — Last 30 Days")
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
                    labels={"day": "Date", "job_count": "Jobs Posted", "sector": "Sector"},
                )
                fig.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    legend=dict(orientation="h", y=1.02, x=1, xanchor="right"),
                    margin=dict(l=0, r=0, t=30, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:
            st.warning(f"Trend chart unavailable: {exc}")

    with col_feed:
        st.subheader("🔔 Latest Postings")
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
                st.info("No recent jobs match your preferences.\n\nGo to **⚙️ Preferences** to set up your feed.")
            else:
                for _, row in df_feed.iterrows():
                    with st.container(border=True):
                        left, right = st.columns([3, 1])
                        with left:
                            st.markdown(f"**{row['job_title']}**")
                            st.caption(f"{row.get('company_or_agency','')} · {_loc(row.to_dict())}")
                            lvl = row.get("experience_level", "")
                            st.caption(f"{LEVEL_LABEL.get(lvl, lvl)} · 📅 {row.get('posted_date','')}")
                        with right:
                            url = (row.get("apply_url") or "").strip()
                            if url:
                                st.link_button("Apply", url, use_container_width=True)
        except Exception as exc:
            st.warning(f"Feed unavailable: {exc}")

    # ── recommended grid ──────────────────────────────────────────────────────
    st.divider()
    st.subheader("🎯 Recommended for You")
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


def _greet() -> str:
    h = date.today().timetuple().tm_hour  # rough approximation
    return "morning" if h < 12 else "afternoon" if h < 18 else "evening"


# ─────────────────────────────────────────────────────────────────────────────
# Job Search
# ─────────────────────────────────────────────────────────────────────────────
def job_search() -> None:
    st.header("🔍 Job Search")

    try:
        opts = get_filter_options()
        all_states     = opts.get("states", [])
        all_categories = opts.get("categories", [])
    except Exception:
        all_states, all_categories = [], []

    # ── filters (left column) ────────────────────────────────────────────────
    fcol, rcol = st.columns([1, 3])

    with fcol:
        st.subheader("Filters")

        # Date range
        st.caption("📅 Posted Date")
        d_from = st.date_input("From", value=date.today() - timedelta(days=30), key="d_from")
        d_to   = st.date_input("To",   value=date.today(),                       key="d_to")

        # Location
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

        # Job type
        st.caption("💼 Job Category")
        sel_cats = st.multiselect("Category", options=all_categories, key="sel_cats")

        # Experience level
        st.caption("🎓 Experience Level")
        sel_levels = st.multiselect(
            "Level",
            options=["entry", "mid", "senior"],
            format_func=lambda x: LEVEL_LABEL.get(x, x),
            key="sel_levels",
        )

        # Sector
        st.caption("🏛 Sector")
        sel_sectors = st.multiselect(
            "Sector",
            options=["federal", "private"],
            format_func=lambda x: "🏛 Federal" if x == "federal" else "🏪 Private",
            key="sel_sectors",
        )

        # Remote type
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

        search_btn = st.button("🔍 Search", type="primary", use_container_width=True)

    # ── results (right column) ────────────────────────────────────────────────
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
                st.info("Use the filters on the left and click **Search**.")
            return

        # ── result header & sort ──────────────────────────────────────────────
        top_l, top_r = st.columns([3, 1])
        with top_l:
            st.success(f"**{len(df)}** jobs found")
        with top_r:
            sort_by = st.selectbox(
                "Sort",
                ["Newest", "Oldest", "Salary ↓", "Salary ↑"],
                label_visibility="collapsed",
            )

        if sort_by == "Oldest":
            df = df.sort_values("posted_date", ascending=True)
        elif sort_by == "Salary ↓":
            df = df.sort_values("salary_max", ascending=False, na_position="last")
        elif sort_by == "Salary ↑":
            df = df.sort_values("salary_min", ascending=True, na_position="last")

        # ── two-column card grid ──────────────────────────────────────────────
        left_col, right_col = st.columns(2)
        for i, (_, row) in enumerate(df.iterrows()):
            with left_col if i % 2 == 0 else right_col:
                job_card(row.to_dict(), key_suffix=f"_s{i}")


# ─────────────────────────────────────────────────────────────────────────────
# Preferences
# ─────────────────────────────────────────────────────────────────────────────
def preferences() -> None:
    st.header("⚙️ My Preferences")
    st.caption("These settings control which jobs appear in your Dashboard feed.")

    prefs = get_preferences(st.session_state.user)
    try:
        opts = get_filter_options()
        all_states     = opts.get("states", [])
    except Exception:
        all_states = []

    with st.form("pref_form"):
        st.subheader("Experience Level(s) to Follow")
        sel_levels = st.multiselect(
            "I want to see:",
            options=["entry", "mid", "senior"],
            default=prefs.get("experience_level", []),
            format_func=lambda x: LEVEL_LABEL.get(x, x),
        )

        st.subheader("Preferred Sector(s)")
        sel_sectors = st.multiselect(
            "Track postings from:",
            options=["federal", "private"],
            default=prefs.get("sector", []),
            format_func=lambda x: "🏛 Federal" if x == "federal" else "🏪 Private",
        )

        st.subheader("Preferred State(s)")
        sel_states = st.multiselect(
            "Focus on:",
            options=all_states,
            default=[s for s in prefs.get("location_state", []) if s in all_states],
        )

        st.subheader("Preferred Work Mode")
        sel_remote = st.multiselect(
            "Mode:",
            options=["remote", "hybrid", "onsite"],
            default=prefs.get("remote_type", []),
            format_func=lambda x: {"remote": "🌐 Remote", "hybrid": "🏠 Hybrid", "onsite": "🏢 On-site"}.get(x, x),
        )

        saved = st.form_submit_button("💾 Save Preferences", type="primary")

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
    st.header("📌 Saved Jobs")
    jobs = get_saved_jobs(st.session_state.user)

    if not jobs:
        st.info("You haven't saved any jobs yet.\n\nUse **🔍 Job Search** or the **Dashboard** feed to save jobs you're interested in.")
        return

    for job in jobs:
        with st.container(border=True):
            left, right = st.columns([4, 1])
            with left:
                st.markdown(f"**{job.get('job_title', 'Untitled')}**")
                st.caption(f"{job.get('company', '')}  ·  {job.get('location', '')}")
                st.caption(f"Saved on {str(job.get('saved_at',''))[:10]}")
            with right:
                url = (job.get("apply_url") or "").strip()
                if url:
                    st.link_button("Apply →", url, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    init_db()

    if not st.session_state.user:
        auth_page()
        return

    sidebar()

    page = st.session_state.page
    if page == "dashboard":
        dashboard()
    elif page == "search":
        job_search()
    elif page == "preferences":
        preferences()
    elif page == "saved":
        saved_jobs()


if __name__ == "__main__":
    main()
