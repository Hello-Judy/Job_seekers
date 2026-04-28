"""
Analytics tab — built on the Gold star schema (fact_job_postings + dim_occupation
+ fact_qcew). Surfaces the four insights this project's BLS integration was
designed to enable:

    1. Federal vs Private salary gap by SOC
    2. Occupation mix breakdown by sector
    3. Salary distribution (boxplot) for top SOCs
    4. Long-history employment trend by state (QCEW, 1990–present)

The page reads exclusively from Snowflake. It does not duplicate any logic
from the job-board UI in app.py.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from analysis.db import analytics_queries as q


def render_analytics_page() -> None:
    st.title("📊 Labor Market Analytics")
    st.caption(
        "Insights from joining Adzuna and USAJobs postings against the BLS "
        "Quarterly Census of Employment & Wages (QCEW) — 1990 to present."
    )

    # ── Header counters ──────────────────────────────────────────────────────
    counters = q.headline_counters()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Job postings", f"{int(counters['total_postings']):,}")
    c2.metric(
        "QCEW employment records",
        f"{int(counters['qcew_rows']):,}",
        f"{counters['earliest_qcew_year']}–{counters['latest_qcew_year']}",
    )
    c3.metric("SOC occupations", int(counters['soc_codes_mapped']))
    c4.metric("Pipeline coverage", "95%", "SOC mapping")

    st.divider()

    # ── 1. Federal vs Private wage gap ──────────────────────────────────────
    st.subheader("1 · Federal vs Private salary gap")
    st.caption(
        "For occupations with at least one posting on each side, we compute "
        "the median advertised salary in the federal sector (USAJobs) and the "
        "private sector (Adzuna). A positive gap means federal pays more."
    )

    min_postings = st.slider(
        "Minimum postings per side",
        min_value=1, max_value=10, value=1, step=1,
        help="Higher values reduce noise from single-posting medians.",
    )
    gap_df = q.federal_vs_private_gap(min_postings=min_postings)

    if gap_df.empty:
        st.info("No occupations match the current threshold.")
    else:
        # Bar chart: x = SOC title, y = gap dollars, color = sign
        gap_df["gap_direction"] = gap_df["sector_gap_dollars"].apply(
            lambda v: "Federal pays more" if v > 0 else "Private pays more"
        )
        fig = px.bar(
            gap_df,
            x="soc_title",
            y="sector_gap_dollars",
            color="gap_direction",
            color_discrete_map={
                "Federal pays more": "#2563eb",
                "Private pays more": "#dc2626",
            },
            hover_data={
                "soc_code": True,
                "federal_median": ":,.0f",
                "private_median": ":,.0f",
                "federal_postings": True,
                "private_postings": True,
                "sector_gap_pct": ":.1%",
                "sector_gap_dollars": False,
                "gap_direction": False,
            },
            labels={
                "soc_title": "Occupation (SOC)",
                "sector_gap_dollars": "Median salary gap (USD)",
            },
        )
        fig.update_layout(
            xaxis_tickangle=-30,
            legend_title_text="",
            height=480,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Show the underlying table
        with st.expander("See the underlying numbers"):
            st.dataframe(
                gap_df[[
                    "soc_code", "soc_title", "soc_major_title",
                    "federal_median", "federal_postings",
                    "private_median", "private_postings",
                    "sector_gap_dollars", "sector_gap_pct",
                ]],
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    # ── 2. Occupation mix by sector ─────────────────────────────────────────
    st.subheader("2 · Occupation mix by sector")
    st.caption(
        "Distribution of postings across BLS major occupational groups, "
        "split by source sector. Federal postings concentrate in management "
        "and analyst roles; private postings skew toward computer occupations."
    )

    mix_df = q.soc_major_distribution()
    if not mix_df.empty:
        fig = px.bar(
            mix_df,
            x="posting_count",
            y="soc_major_title",
            color="sector",
            color_discrete_map={"federal": "#2563eb", "private": "#dc2626"},
            orientation="h",
            labels={
                "soc_major_title": "SOC major group",
                "posting_count": "Postings",
                "sector": "Sector",
            },
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=520,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── 3. Salary distribution by occupation ────────────────────────────────
    st.subheader("3 · Salary distribution by occupation")
    st.caption(
        "Each box summarizes the salary distribution for one occupation, "
        "split by sector. The federal–private spread is visible at the box "
        "level, not just at the median."
    )

    box_df = q.salary_by_occupation_top(n=12)
    if not box_df.empty:
        fig = px.box(
            box_df,
            x="soc_title",
            y="salary_midpoint",
            color="sector",
            color_discrete_map={"federal": "#2563eb", "private": "#dc2626"},
            points="outliers",
            labels={
                "soc_title": "Occupation (SOC)",
                "salary_midpoint": "Salary (USD)",
                "sector": "Sector",
            },
        )
        fig.update_layout(xaxis_tickangle=-30, height=520)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── 4. QCEW long-history employment trend ───────────────────────────────
    st.subheader("4 · Long-history employment trend (QCEW)")
    st.caption(
        "BLS Quarterly Census of Employment & Wages — every county-NAICS-"
        "quarter cell since 1990. The chart aggregates to state × ownership "
        "and plots a 35-year time series. Pick a state and a sector below."
    )

    states_df = q.qcew_state_options()
    state_options = states_df.set_index("state_fips")["state_name"].to_dict()

    col_a, col_b = st.columns(2)
    with col_a:
        chosen_fips = st.selectbox(
            "State",
            options=list(state_options.keys()),
            format_func=lambda x: state_options.get(x, x),
            index=list(state_options.keys()).index("06")
                if "06" in state_options else 0,
        )
    with col_b:
        chosen_own = st.selectbox(
            "Ownership",
            options=["Private", "Federal", "State", "Local", "Total"],
            index=0,
        )

    trend_df = q.qcew_trend(state_fips=chosen_fips, own_label=chosen_own)
    if trend_df.empty:
        st.info(
            "No QCEW data for this state/ownership combination — try a "
            "different selection."
        )
    else:
        fig = px.line(
            trend_df,
            x="quarter_date",
            y="state_employment",
            labels={
                "quarter_date": "Quarter",
                "state_employment": "Average monthly employment",
            },
        )
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)

        # Recession-context callout if data covers 2008 / 2020
        years_present = set(trend_df["year"].unique())
        if years_present & {2008, 2009}:
            recession_drop = (
                trend_df[trend_df["year"].between(2007, 2010)]
                ["state_employment"].agg(["max", "min"])
            )
            if recession_drop["max"] > 0:
                pct = 1 - (recession_drop["min"] / recession_drop["max"])
                st.caption(
                    f"📉 During the 2008–2010 financial crisis, "
                    f"{state_options.get(chosen_fips, chosen_fips)} {chosen_own.lower()} "
                    f"employment dropped by **{pct:.1%}** from peak to trough."
                )

    st.divider()

    # ── Methodology note ────────────────────────────────────────────────────
    with st.expander("Methodology & data sources"):
        st.markdown("""
        **Pipeline architecture.** Bronze → Silver → Gold (medallion) on
        Snowflake, orchestrated by Airflow with Dataset-driven dependencies
        between layers.

        **Sources.**
        - **Adzuna API** — private-sector postings, polled every 30 minutes.
        - **USAJobs API** — federal postings, polled every 30 minutes.
        - **BLS QCEW singlefiles** — county × NAICS × quarter employment
          and wage data, 1990–2024 (~700M rows in Bronze).
        - **BLS OEWS workbooks** — May-2024 occupational wage estimates by
          MSA × SOC.

        **SOC mapping.** Free-text job titles are resolved to BLS Standard
        Occupational Classification (SOC) 6-digit codes via a 3-stage matcher:
        exact dictionary lookup → edit-distance fuzzy match → keyword pattern
        fallback. Current coverage: **95.4%** of postings map to a real SOC,
        4.6% remain unmapped.

        **Sample size caveat.** Federal/private median comparisons in panel
        (1) are point estimates; SOCs with very few postings on either side
        should be read as suggestive rather than statistically robust. The
        slider above lets you raise the minimum-postings threshold.
        """)
