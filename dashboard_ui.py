"""
dashboard_ui.py
===============
Professional Tableau & Power BI Style Interactive Decision Dashboard.
Provides executive-grade visual design, dark/light theme switching,
interactive Plotly visualizations, filter slicers, and scenario simulation.
"""

from __future__ import annotations

import json
from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st


# ===============================================================================
# PALETTES & THEME DEFINITIONS
# ===============================================================================
THEME_CONFIG = {
    "dark": {
        "bg_app": "#0B0F19",
        "bg_surface": "#131C2E",
        "bg_card": "#182337",
        "bg_card_hover": "#1E2D47",
        "border": "#24324D",
        "border_subtle": "#1B273E",
        "text_primary": "#F8FAFC",
        "text_secondary": "#94A3B8",
        "text_muted": "#64748B",
        "accent_primary": "#00E5FF",      # Cyber Cyan
        "accent_secondary": "#38BDF8",    # Sky Blue
        "accent_success": "#10B981",      # Emerald
        "accent_warning": "#F59E0B",      # Amber
        "accent_danger": "#F43F5E",       # Coral Rose
        "accent_purple": "#A855F7",       # Purple
        "chart_palette": ["#00E5FF", "#38BDF8", "#818CF8", "#A855F7", "#F43F5E", "#F59E0B", "#10B981", "#34D399"],
        "grid_color": "#1F2D44",
        "plot_bg": "#131C2E",
        "paper_bg": "#182337",
        "plotly_template": "plotly_dark",
    },
    "light": {
        "bg_app": "#F8FAFC",
        "bg_surface": "#FFFFFF",
        "bg_card": "#FFFFFF",
        "bg_card_hover": "#F1F5F9",
        "border": "#E2E8F0",
        "border_subtle": "#CBD5E1",
        "text_primary": "#0F172A",
        "text_secondary": "#475569",
        "text_muted": "#94A3B8",
        "accent_primary": "#0078D4",      # Power BI Blue
        "accent_secondary": "#4E79A7",    # Tableau Classic Blue
        "accent_success": "#107C41",      # Forest Green
        "accent_warning": "#D83B01",      # Power BI Warm Red
        "accent_danger": "#E15759",       # Tableau Coral
        "accent_purple": "#5C2D91",       # Deep Violet
        "chart_palette": ["#0078D4", "#F28E2B", "#107C41", "#E15759", "#76B7B2", "#59A14F", "#EDC948", "#B07AA1"],
        "grid_color": "#F1F5F9",
        "plot_bg": "#FFFFFF",
        "paper_bg": "#FFFFFF",
        "plotly_template": "plotly_white",
    }
}


def inject_theme_css(is_dark: bool = False):
    """Injects high-fidelity Tableau / Power BI CSS theme variables and hides the top header bar."""
    cfg = THEME_CONFIG["dark" if is_dark else "light"]

    css = f"""
    <style>
        :root {{
            --bi-bg-app: {cfg["bg_app"]};
            --bi-bg-surface: {cfg["bg_surface"]};
            --bi-bg-card: {cfg["bg_card"]};
            --bi-bg-card-hover: {cfg["bg_card_hover"]};
            --bi-border: {cfg["border"]};
            --bi-border-subtle: {cfg["border_subtle"]};
            --bi-text-primary: {cfg["text_primary"]};
            --bi-text-secondary: {cfg["text_secondary"]};
            --bi-text-muted: {cfg["text_muted"]};
            --bi-accent-primary: {cfg["accent_primary"]};
            --bi-accent-success: {cfg["accent_success"]};
            --bi-accent-warning: {cfg["accent_warning"]};
            --bi-accent-danger: {cfg["accent_danger"]};
        }}

        /* Completely remove and hide the top Streamlit header bar (white bar) */
        header[data-testid="stHeader"],
        [data-testid="stHeader"],
        .stApp > header {{
            display: none !important;
            visibility: hidden !important;
            height: 0px !important;
            background: transparent !important;
        }}
        #MainMenu {{ visibility: hidden !important; }}
        footer {{ visibility: hidden !important; }}
        [data-testid="stDecoration"] {{ display: none !important; }}
        .stAppDeployButton {{ display: none !important; }}
        div[data-testid="stToolbar"] {{ display: none !important; }}
        div[data-testid="stSidebarCollapseButton"] {{
            top: 10px !important;
        }}

        /* App canvas & typography */
        .stApp {{
            background-color: var(--bi-bg-app);
            color: var(--bi-text-primary);
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
            margin-top: 0px !important;
            padding-top: 0px !important;
        }}

        .block-container {{
            padding-top: 1.2rem !important;
            padding-bottom: 2rem !important;
            max-width: 100% !important;
        }}

        /* BI Ribbon Header */
        .bi-ribbon {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 12px 20px;
            background: {cfg["bg_surface"]};
            border: 1px solid var(--bi-border);
            border-radius: 8px;
            margin-bottom: 18px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        }}

        .bi-ribbon-title {{
            font-size: 19px;
            font-weight: 700;
            color: var(--bi-text-primary);
            letter-spacing: -0.3px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .bi-ribbon-tag {{
            font-size: 11px;
            font-weight: 600;
            padding: 3px 8px;
            border-radius: 4px;
            background: rgba(0, 120, 212, 0.1);
            color: var(--bi-accent-primary);
            border: 1px solid var(--bi-accent-primary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        /* Slicer Container */
        .bi-slicer-container {{
            background-color: var(--bi-bg-surface);
            border: 1px solid var(--bi-border);
            border-radius: 8px;
            padding: 14px 16px;
            margin-bottom: 18px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.03);
        }}

        .bi-slicer-header {{
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            color: var(--bi-text-secondary);
            margin-bottom: 10px;
            display: flex;
            align-items: center;
            gap: 6px;
        }}

        /* Hero KPI Tile */
        .bi-kpi-card {{
            background: linear-gradient(145deg, var(--bi-bg-card) 0%, var(--bi-bg-surface) 100%);
            border: 1px solid var(--bi-border);
            border-top: 3px solid var(--bi-accent-primary);
            border-radius: 8px;
            padding: 16px 18px;
            margin-bottom: 12px;
            transition: transform 0.15s ease, box-shadow 0.15s ease;
            box-shadow: 0 4px 12px rgba(0,0,0,{0.25 if is_dark else 0.05});
        }}

        .bi-kpi-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(0,0,0,{0.35 if is_dark else 0.08});
            border-color: var(--bi-accent-primary);
        }}

        .bi-kpi-title {{
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.7px;
            color: var(--bi-text-secondary);
            margin-bottom: 6px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .bi-kpi-value {{
            font-size: 26px;
            font-weight: 800;
            color: var(--bi-text-primary);
            line-height: 1.1;
            margin-bottom: 6px;
            font-family: 'Segoe UI', -apple-system, sans-serif;
        }}

        .bi-kpi-delta {{
            font-size: 11px;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 2px 6px;
            border-radius: 4px;
        }}

        .bi-delta-pos {{
            background: rgba(16, 185, 129, 0.15);
            color: var(--bi-accent-success);
        }}

        .bi-delta-neu {{
            background: rgba(148, 163, 184, 0.15);
            color: var(--bi-text-secondary);
        }}

        /* Section Card Wrapper */
        .bi-chart-card {{
            background-color: var(--bi-bg-card);
            border: 1px solid var(--bi-border);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 16px;
            box-shadow: 0 2px 10px rgba(0,0,0,{0.2 if is_dark else 0.04});
        }}

        .bi-chart-header {{
            font-size: 14px;
            font-weight: 700;
            color: var(--bi-text-primary);
            margin-bottom: 2px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }}

        .bi-chart-sub {{
            font-size: 11px;
            color: var(--bi-text-secondary);
            margin-bottom: 12px;
        }}

        /* Simulator Card */
        .bi-sim-box {{
            background: linear-gradient(135deg, rgba(0, 229, 255, 0.05) 0%, rgba(56, 189, 248, 0.02) 100%);
            border: 1px solid {cfg["border"]};
            border-left: 4px solid var(--bi-accent-primary);
            border-radius: 8px;
            padding: 16px;
            margin-top: 10px;
        }}

        /* Navigation Bar / Streamlit Tabs Styling */
        div[data-baseweb="tab-list"] {{
            gap: 6px !important;
            background-color: transparent !important;
            border-bottom: 2px solid #E2E8F0 !important;
            padding-bottom: 4px !important;
            margin-bottom: 16px !important;
        }}

        button[data-baseweb="tab"] {{
            background-color: transparent !important;
            border: 1px solid transparent !important;
            border-radius: 6px !important;
            padding: 8px 16px !important;
            transition: all 0.15s ease-in-out !important;
        }}

        button[data-baseweb="tab"] p,
        button[data-baseweb="tab"] span,
        button[data-baseweb="tab"] div {{
            color: #334155 !important; /* Clearly visible dark slate text! */
            font-size: 13px !important;
            font-weight: 600 !important;
        }}

        button[data-baseweb="tab"]:hover {{
            background-color: #F1F5F9 !important;
        }}

        button[data-baseweb="tab"]:hover p,
        button[data-baseweb="tab"]:hover span {{
            color: #0078D4 !important;
        }}

        button[data-baseweb="tab"][aria-selected="true"] {{
            background-color: #FFFFFF !important;
            border: 1px solid #CBD5E1 !important;
            border-bottom: 2px solid #0078D4 !important;
            box-shadow: 0 2px 6px rgba(0,0,0,0.04) !important;
        }}

        button[data-baseweb="tab"][aria-selected="true"] p,
        button[data-baseweb="tab"][aria-selected="true"] span {{
            color: #0078D4 !important;
            font-weight: 700 !important;
        }}

        /* Force Light Background for All Input Controls */
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        div[data-baseweb="base-input"],
        .stDateInput input,
        .stTextInput input,
        .stSelectbox div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="select"] > div {{
            background-color: #FFFFFF !important;
            color: #0F172A !important;
            border: 1px solid #CBD5E1 !important;
            border-radius: 6px !important;
            box-shadow: none !important;
        }}

        /* Clean styling for Multiselect Selected Chips */
        span[data-baseweb="tag"] {{
            background-color: #EFF6FF !important;
            color: #1D4ED8 !important;
            border: 1px solid #BFDBFE !important;
            border-radius: 4px !important;
            font-weight: 600 !important;
        }}

        span[data-baseweb="tag"] span {{
            color: #1D4ED8 !important;
        }}

        span[data-baseweb="tag"] svg {{
            fill: #1D4ED8 !important;
        }}

        /* Input typography & placeholders */
        input, .stDateInput input {{
            color: #0F172A !important;
            font-weight: 500 !important;
        }}

        /* Dropdown popups & menus */
        ul[data-baseweb="menu"],
        div[data-baseweb="popover"] > div {{
            background-color: #FFFFFF !important;
            color: #0F172A !important;
            border: 1px solid #CBD5E1 !important;
            border-radius: 6px !important;
            box-shadow: 0 4px 16px rgba(0,0,0,0.08) !important;
        }}

        li[data-baseweb="menu-item"] {{
            color: #0F172A !important;
        }}

        li[data-baseweb="menu-item"]:hover {{
            background-color: #F1F5F9 !important;
        }}

        /* Calendar Popover */
        div[data-baseweb="calendar"],
        div[data-baseweb="calendar"] * {{
            color: #0F172A !important;
            background-color: #FFFFFF !important;
        }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def build_plotly_layout(cfg: dict, title: str = "", height: int = 360, is_dark: bool = True) -> dict:
    """Standardizes Tableau / Power BI layout configuration for Plotly."""
    return {
        "title": {
            "text": title,
            "font": {"size": 14, "color": cfg["text_primary"], "family": "Segoe UI, sans-serif"},
            "x": 0.01,
            "y": 0.98,
        },
        "height": height,
        "margin": {"l": 45, "r": 35, "t": 40 if title else 20, "b": 35},
        "plot_bgcolor": cfg["plot_bg"],
        "paper_bgcolor": cfg["paper_bg"],
        "font": {"color": cfg["text_secondary"], "family": "Segoe UI, sans-serif", "size": 11},
        "xaxis": {
            "gridcolor": cfg["grid_color"],
            "zerolinecolor": cfg["border"],
            "showline": True,
            "linecolor": cfg["border"],
            "tickfont": {"size": 10, "color": cfg["text_secondary"]},
        },
        "yaxis": {
            "gridcolor": cfg["grid_color"],
            "zerolinecolor": cfg["border"],
            "showline": True,
            "linecolor": cfg["border"],
            "tickfont": {"size": 10, "color": cfg["text_secondary"]},
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1.0,
            "font": {"size": 10, "color": cfg["text_secondary"]},
        },
        "hoverlabel": {
            "bgcolor": "#1E293B" if is_dark else "#0F172A",
            "font": {"color": "#FFFFFF", "size": 11, "family": "Segoe UI, monospace"},
            "bordercolor": cfg["border"],
        },
    }


# ===============================================================================
# DATA FILTERING & AGGREGATION ENGINE
# ===============================================================================
@st.cache_data(show_spinner=False)
def prepare_base_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Pre-computes optimized date parts and transaction dimensions."""
    data = df.copy()
    data["date_dt"] = pd.to_datetime(data["date"], errors="coerce")
    data["year_month"] = data["date_dt"].dt.to_period("M").astype(str)
    data["month_name"] = data["date_dt"].dt.strftime("%b %Y")
    data["day_of_week"] = data["date_dt"].dt.day_name()
    data["dow_num"] = data["date_dt"].dt.dayofweek
    data["month_num"] = data["date_dt"].dt.month
    data["quarter"] = "Q" + data["date_dt"].dt.quarter.astype(str) + " " + data["date_dt"].dt.year.astype(str)

    # Price Tier categorization
    conditions = [
        data["unit_price"] < 2.0,
        (data["unit_price"] >= 2.0) & (data["unit_price"] <= 10.0),
        data["unit_price"] > 10.0,
    ]
    choices = ["Economy (<£2)", "Mid-Range (£2-£10)", "Premium (>£10)"]
    data["price_tier"] = np.select(conditions, choices, default="Mid-Range (£2-£10)")

    return data


def filter_sales_data(
    df: pd.DataFrame,
    selected_countries: list[str],
    selected_quarters: list[str],
    selected_tiers: list[str],
    date_range: tuple,
) -> pd.DataFrame:
    """Fast vectorized filtering based on slicer inputs."""
    filtered = df

    if selected_countries and "All Markets" not in selected_countries:
        filtered = filtered[filtered["country"].isin(selected_countries)]

    if selected_quarters and "All Quarters" not in selected_quarters:
        filtered = filtered[filtered["quarter"].isin(selected_quarters)]

    if selected_tiers and "All Tiers" not in selected_tiers:
        filtered = filtered[filtered["price_tier"].isin(selected_tiers)]

    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered = filtered[
            (filtered["date_dt"].dt.date >= start_date)
            & (filtered["date_dt"].dt.date <= end_date)
        ]

    return filtered


# ===============================================================================
# MAIN TABLEAU / POWER BI DASHBOARD COMPONENT
# ===============================================================================
def render_decision_dashboard(
    df_transactions: pd.DataFrame,
    df_products: pd.DataFrame,
    dataset_metadata: dict,
    is_dark: bool = False,
):
    """
    Renders the complete Tableau / Power BI Decision Dashboard interface.
    """
    cfg = THEME_CONFIG["dark" if is_dark else "light"]

    # 1. Prepare data with caching
    prepared_tx = prepare_base_transactions(df_transactions)

    # 2. Top Executive Ribbon / Power BI Workspace Header
    st.markdown(f"""
    <div class="bi-ribbon">
        <div class="bi-ribbon-title">
            <span>Executive Decision Space</span>
            <span class="bi-ribbon-tag"></span>
        </div>
        <div style="font-size: 11px; color: {cfg['text_secondary']}; display: flex; gap: 14px; align-items: center;">
            <span>Dataset: <b>UCI Retail</b></span>
            <span>Period: <b>{dataset_metadata.get('start_date')} → {dataset_metadata.get('end_date')}</b></span>
            <span>Currency: <b>GBP (£)</b></span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 3. Interactive Slicers Panel (Power BI Filter Bar)
    with st.container():
        st.markdown("""
        <div class="bi-slicer-header">
            <span></span>
        </div>
        """, unsafe_allow_html=True)

        s_col1, s_col2, s_col3, s_col4 = st.columns([1.5, 1.2, 1.1, 1.2])

        all_countries = ["All Markets"] + sorted(prepared_tx["country"].dropna().unique().tolist())
        all_quarters = ["All Quarters"] + sorted(prepared_tx["quarter"].dropna().unique().tolist())
        tier_options = ["All Tiers", "Economy (<£2)", "Mid-Range (£2-£10)", "Premium (>£10)"]

        with s_col1:
            selected_countries = st.multiselect(
                "🌍 Market / Country Slicer",
                options=all_countries,
                default=["All Markets"],
                help="Filter by single or multiple international trade regions"
            )

        with s_col2:
            selected_quarters = st.multiselect(
                "📅 Calendar Period (Quarter)",
                options=all_quarters,
                default=["All Quarters"],
                help="Select fiscal quarters to slice trends"
            )

        with s_col3:
            selected_tiers = st.multiselect(
                "🏷️ Product Price Tier",
                options=tier_options,
                default=["All Tiers"],
                help="Slice by Economy, Mid-Range, or Premium catalogs"
            )

        with s_col4:
            min_date = prepared_tx["date_dt"].min().date()
            max_date = prepared_tx["date_dt"].max().date()
            selected_date_range = st.date_input(
                "📆 Fine Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                help="Adjust timeline bounds"
            )

    # Apply slicers
    filtered_tx = filter_sales_data(
        prepared_tx,
        selected_countries=selected_countries,
        selected_quarters=selected_quarters,
        selected_tiers=selected_tiers,
        date_range=selected_date_range if isinstance(selected_date_range, (tuple, list)) and len(selected_date_range) == 2 else (min_date, max_date)
    )

    if filtered_tx.empty:
        st.warning("⚠️ No transactions match the current filter selection. Please reset your slicers.")
        return

    # 4. Hero KPI Metric Cards (Power BI Card Grid)
    tot_rev = filtered_tx["total_revenue"].sum()
    tot_units = filtered_tx["quantity"].sum()
    tot_orders = filtered_tx["tx_id"].nunique()
    tot_customers = filtered_tx["customer_id"].dropna().nunique()
    avg_order_val = tot_rev / max(1, tot_orders)
    avg_price_realized = tot_rev / max(1, tot_units)

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    with kpi1:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: {cfg['accent_primary']};">
            <div class="bi-kpi-title">
                <span>Net Invoiced Revenue</span>
                <span></span>
            </div>
            <div class="bi-kpi-value">£{tot_rev:,.0f}</div>
            <div class="bi-kpi-delta bi-delta-pos">
                <span>▲ 100% Sliced</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with kpi2:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: {cfg['accent_secondary']};">
            <div class="bi-kpi-title">
                <span>Physical Units Sold</span>
                <span></span>
            </div>
            <div class="bi-kpi-value">{tot_units:,.0f}</div>
            <div class="bi-kpi-delta bi-delta-neu">
                <span>{tot_units / max(1, tot_orders):.1f} units / invoice</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with kpi3:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: {cfg['accent_success']};">
            <div class="bi-kpi-title">
                <span>Completed Invoices</span>
                <span></span>
            </div>
            <div class="bi-kpi-value">{tot_orders:,}</div>
            <div class="bi-kpi-delta bi-delta-pos">
                <span>Unique Orders</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with kpi4:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: {cfg['accent_warning']};">
            <div class="bi-kpi-title">
                <span>Average Order Value</span>
                <span></span>
            </div>
            <div class="bi-kpi-value">£{avg_order_val:,.2f}</div>
            <div class="bi-kpi-delta bi-delta-neu">
                <span>Per transaction ticket</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with kpi5:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: {cfg['accent_purple']};">
            <div class="bi-kpi-title">
                <span>Active Customer Base</span>
                <span></span>
            </div>
            <div class="bi-kpi-value">{tot_customers:,}</div>
            <div class="bi-kpi-delta bi-delta-pos">
                <span>£{avg_price_realized:.2f} avg/unit</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ===============================================================================
    # ROW 1: Tableau Combi Visual (Revenue Bar + Demand Units Line) & Regional Treemap/Bar
    # ===============================================================================
    col_chart_left, col_chart_right = st.columns([1.6, 1.0])

    with col_chart_left:
        # Aggregation by month
        monthly_grp = (
            filtered_tx.groupby("year_month", as_index=False)
            .agg(
                revenue=("total_revenue", "sum"),
                units=("quantity", "sum"),
                invoices=("tx_id", "nunique"),
            )
            .sort_values("year_month")
        )

        fig_combi = make_subplots(
            specs=[[{"secondary_y": True}]],
        )

        # Revenue Bars
        fig_combi.add_trace(
            go.Bar(
                x=monthly_grp["year_month"],
                y=monthly_grp["revenue"],
                name="Net Revenue (£)",
                marker=dict(
                    color=cfg["accent_primary"],
                    opacity=0.85,
                    line=dict(color=cfg["accent_primary"], width=1),
                ),
                hovertemplate="<b>%{x}</b><br>Revenue: £%{y:,.0f}<extra></extra>",
            ),
            secondary_y=False,
        )

        # Demand Units Spline Line
        fig_combi.add_trace(
            go.Scatter(
                x=monthly_grp["year_month"],
                y=monthly_grp["units"],
                name="Demand Units Sold",
                mode="lines+markers",
                line=dict(color=cfg["accent_warning"], width=3, shape="spline"),
                marker=dict(size=7, color=cfg["accent_warning"], symbol="diamond"),
                hovertemplate="<b>%{x}</b><br>Units: %{y:,.0f}<extra></extra>",
            ),
            secondary_y=True,
        )

        layout_combi = build_plotly_layout(
            cfg,
            title="📈 Revenue Dynamics & Physical Demand Velocity (Dual-Axis Combi)",
            height=370,
            is_dark=is_dark,
        )
        layout_combi["yaxis"]["title"] = "Revenue (£)"
        layout_combi["yaxis2"] = {
            "title": "Demand Units",
            "overlaying": "y",
            "side": "right",
            "gridcolor": "rgba(0,0,0,0)",
            "showline": True,
            "linecolor": cfg["border"],
            "tickfont": {"size": 10, "color": cfg["text_secondary"]},
        }
        fig_combi.update_layout(layout_combi)
        st.plotly_chart(fig_combi, use_container_width=True)

    with col_chart_right:
        # Top Markets by Revenue
        country_grp = (
            filtered_tx.groupby("country", as_index=False)["total_revenue"]
            .sum()
            .sort_values("total_revenue", ascending=False)
            .head(8)
        )
        country_grp["pct_share"] = (country_grp["total_revenue"] / tot_rev) * 100

        fig_country = go.Figure(
            go.Bar(
                x=country_grp["total_revenue"],
                y=country_grp["country"],
                orientation="h",
                marker=dict(
                    color=country_grp["total_revenue"],
                    colorscale="Tealgrn" if not is_dark else "Viridis",
                    showscale=False,
                    line=dict(color=cfg["border"], width=1),
                ),
                text=country_grp["pct_share"].apply(lambda p: f"{p:.1f}%"),
                textposition="auto",
                hovertemplate="<b>%{y}</b><br>Revenue: £%{x:,.0f}<br>Share: %{text}<extra></extra>",
            )
        )
        layout_country = build_plotly_layout(
            cfg,
            title="🌍 Top Market Penetration (% Revenue Share)",
            height=370,
            is_dark=is_dark,
        )
        layout_country["yaxis"]["autorange"] = "reversed"
        layout_country["xaxis"]["title"] = "Total Revenue (£)"
        fig_country.update_layout(layout_country)
        st.plotly_chart(fig_country, use_container_width=True)

    # ===============================================================================
    # ROW 2: Portfolio Matrix (Quad Scatter) & Commercial Velocity Heatmap
    # ===============================================================================
    col_quad, col_heat = st.columns([1.2, 1.2])

    with col_quad:
        # Aggregate top 80 products for readable scatter
        prod_agg = (
            filtered_tx.groupby("product_id", as_index=False)
            .agg(
                units_sold=("quantity", "sum"),
                revenue=("total_revenue", "sum"),
                avg_price=("unit_price", "median"),
            )
            .merge(df_products[["product_id", "name"]], on="product_id", how="left")
            .nlargest(60, "revenue")
        )
        prod_agg["short_name"] = prod_agg["name"].fillna("SKU " + prod_agg["product_id"].astype(str)).str.slice(0, 25)

        fig_quad = px.scatter(
            prod_agg,
            x="avg_price",
            y="units_sold",
            size="revenue",
            color="units_sold",
            hover_name="short_name",
            color_continuous_scale="Turbo" if is_dark else "Blues",
            labels={"avg_price": "Median Unit Price (£)", "units_sold": "Units Sold", "revenue": "Total Revenue (£)"},
            size_max=36,
        )

        layout_quad = build_plotly_layout(
            cfg,
            title="🎯 SKU Demand-Price Positioning Matrix (BCG-Style Bubble Quad)",
            height=380,
            is_dark=is_dark,
        )
        fig_quad.update_layout(layout_quad)
        fig_quad.update_coloraxes(showscale=False)

        # Median reference lines for quadrant segmentation
        med_p = prod_agg["avg_price"].median()
        med_u = prod_agg["units_sold"].median()
        fig_quad.add_vline(x=med_p, line_dash="dash", line_color=cfg["text_muted"], opacity=0.5)
        fig_quad.add_hline(y=med_u, line_dash="dash", line_color=cfg["text_muted"], opacity=0.5)

        st.plotly_chart(fig_quad, use_container_width=True)

    with col_heat:
        # Heatmap of transactions by Month vs Day of Week
        days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Sunday"]
        heat_data = (
            filtered_tx.groupby(["year_month", "day_of_week"])["total_revenue"]
            .sum()
            .reset_index()
        )
        pivot_heat = heat_data.pivot(index="day_of_week", columns="year_month", values="total_revenue").fillna(0)
        # Reindex days
        available_days = [d for d in days_order if d in pivot_heat.index]
        pivot_heat = pivot_heat.reindex(available_days)

        fig_heat = px.imshow(
            pivot_heat,
            labels=dict(x="Month", y="Day of Week", color="Revenue (£)"),
            x=pivot_heat.columns,
            y=pivot_heat.index,
            aspect="auto",
            color_continuous_scale="Plasma" if is_dark else "Purples",
        )
        layout_heat = build_plotly_layout(
            cfg,
            title="🔥 Commercial Trading Velocity Matrix (Day vs Month Heat)",
            height=380,
            is_dark=is_dark,
        )
        fig_heat.update_layout(layout_heat)
        st.plotly_chart(fig_heat, use_container_width=True)

    # ===============================================================================
    # ROW 3: Pareto 80/20 Analysis & Ticket Size Distribution
    # ===============================================================================
    col_pareto, col_dist = st.columns([1.4, 1.0])

    with col_pareto:
        # Pareto Chart: Top 25 products
        pareto_df = (
            filtered_tx.groupby("product_id", as_index=False)["total_revenue"]
            .sum()
            .merge(df_products[["product_id", "name"]], on="product_id", how="left")
            .sort_values("total_revenue", ascending=False)
            .head(25)
        )
        pareto_df["display_label"] = pareto_df["name"].fillna("SKU " + pareto_df["product_id"].astype(str)).str.slice(0, 18)
        pareto_df["cum_revenue"] = pareto_df["total_revenue"].cumsum()
        pareto_df["cum_pct"] = (pareto_df["cum_revenue"] / tot_rev) * 100

        fig_pareto = make_subplots(specs=[[{"secondary_y": True}]])

        fig_pareto.add_trace(
            go.Bar(
                x=pareto_df["display_label"],
                y=pareto_df["total_revenue"],
                name="Product Revenue (£)",
                marker=dict(color=cfg["accent_secondary"], opacity=0.85),
                hovertemplate="<b>%{x}</b><br>Revenue: £%{y:,.0f}<extra></extra>",
            ),
            secondary_y=False,
        )

        fig_pareto.add_trace(
            go.Scatter(
                x=pareto_df["display_label"],
                y=pareto_df["cum_pct"],
                name="Cumulative Revenue %",
                mode="lines+markers",
                line=dict(color=cfg["accent_danger"], width=2.5),
                marker=dict(size=6, color=cfg["accent_danger"]),
                hovertemplate="<b>%{x}</b><br>Cumulative: %{y:.1f}%<extra></extra>",
            ),
            secondary_y=True,
        )

        # 80% benchmark line
        fig_pareto.add_hline(
            y=80,
            line_dash="dot",
            line_color=cfg["accent_warning"],
            secondary_y=True,
            annotation_text="80% Revenue Cutoff",
            annotation_position="bottom right",
            annotation_font=dict(color=cfg["accent_warning"], size=10),
        )

        layout_pareto = build_plotly_layout(
            cfg,
            title="📊 Pareto 80/20 SKU Revenue Concentration",
            height=360,
            is_dark=is_dark,
        )
        layout_pareto["xaxis"]["tickangle"] = -40
        layout_pareto["yaxis"]["title"] = "Revenue (£)"
        layout_pareto["yaxis2"] = {
            "title": "Cumulative %",
            "overlaying": "y",
            "side": "right",
            "range": [0, 105],
            "gridcolor": "rgba(0,0,0,0)",
            "tickfont": {"size": 10, "color": cfg["text_secondary"]},
        }
        fig_pareto.update_layout(layout_pareto)
        st.plotly_chart(fig_pareto, use_container_width=True)

    with col_dist:
        # Invoice Value Distribution
        invoice_totals = (
            filtered_tx.groupby("tx_id")["total_revenue"]
            .sum()
            .clip(upper=1500)  # Clip outliers for clean distribution visual
        )

        fig_dist = px.histogram(
            invoice_totals,
            nbins=30,
            color_discrete_sequence=[cfg["accent_purple"]],
            labels={"value": "Invoice Value (£, clipped at £1,500)"},
        )
        layout_dist = build_plotly_layout(
            cfg,
            title="🏷️ Invoice Ticket Value Distribution",
            height=360,
            is_dark=is_dark,
        )
        layout_dist["yaxis"]["title"] = "Transaction Frequency"
        layout_dist["xaxis"]["title"] = "Invoice Total (£)"
        layout_dist["showlegend"] = False
        fig_dist.update_layout(layout_dist)
        st.plotly_chart(fig_dist, use_container_width=True)

    # ===============================================================================
    # ROW 4: Power BI Pivot Grid & What-If Commercial Simulator
    # ===============================================================================
    col_grid, col_sim = st.columns([1.3, 1.1])

    with col_grid:
        st.markdown("""
        <div class="bi-chart-header">
            <span>📋 Strategic Product Performance Leaderboard</span>
        </div>
        <div class="bi-chart-sub">
            Sortable multi-dimensional catalog rankings with revenue contribution share.
        </div>
        """, unsafe_allow_html=True)

        grid_summary = (
            filtered_tx.groupby("product_id", as_index=False)
            .agg(
                units_sold=("quantity", "sum"),
                total_revenue=("total_revenue", "sum"),
                orders_count=("tx_id", "nunique"),
                avg_unit_price=("unit_price", "median"),
            )
            .merge(df_products[["product_id", "name"]], on="product_id", how="left")
            .sort_values("total_revenue", ascending=False)
            .head(30)
        )
        grid_summary["revenue_share_pct"] = (grid_summary["total_revenue"] / tot_rev) * 100
        grid_summary["product_name"] = grid_summary["name"].fillna("Unknown SKU")

        display_grid = grid_summary[[
            "product_id", "product_name", "total_revenue", "units_sold",
            "avg_unit_price", "orders_count", "revenue_share_pct"
        ]].copy()

        st.dataframe(
            display_grid,
            column_config={
                "product_id": st.column_config.TextColumn("SKU ID", width="small"),
                "product_name": st.column_config.TextColumn("Product Title", width="large"),
                "total_revenue": st.column_config.ProgressColumn(
                    "Total Revenue (£)",
                    format="£%d",
                    min_value=0,
                    max_value=float(grid_summary["total_revenue"].max()),
                ),
                "units_sold": st.column_config.NumberColumn("Units", format="%d"),
                "avg_unit_price": st.column_config.NumberColumn("Median Price", format="£%.2f"),
                "orders_count": st.column_config.NumberColumn("Invoices", format="%d"),
                "revenue_share_pct": st.column_config.NumberColumn("Revenue Share", format="%.2f%%"),
            },
            hide_index=True,
            use_container_width=True,
            height=360,
        )

    with col_sim:
        st.markdown("""
        <div class="bi-chart-header">
            <span>🎛️ What-If Commercial & Demand Scenario Sandbox</span>
        </div>
        <div class="bi-chart-sub">
            Simulate price elasticity, demand shift, and marketing impact on sliced portfolio revenue.
        </div>
        """, unsafe_allow_html=True)

        with st.container():
            sim_c1, sim_c2 = st.columns(2)
            with sim_c1:
                price_shift_pct = st.slider(
                    "Price Adjustment (%)",
                    min_value=-25,
                    max_value=35,
                    value=0,
                    step=5,
                    help="Adjust price up or down across the filtered catalog",
                )
                elasticity = st.slider(
                    "Demand Elasticity (ε)",
                    min_value=-2.5,
                    max_value=-0.2,
                    value=-1.1,
                    step=0.1,
                    help="Price elasticity of demand (typically -0.8 to -1.5)",
                )
            with sim_c2:
                marketing_uplift = st.slider(
                    "Marketing Lift (%)",
                    min_value=0,
                    max_value=40,
                    value=5,
                    step=5,
                    help="Incremental volume boost from promotional campaigns",
                )

            # Calculation
            # % Change in Q = (Elasticity * % Change in P) + Marketing Uplift
            demand_pct_change = (elasticity * (price_shift_pct / 100.0)) + (marketing_uplift / 100.0)
            simulated_units = max(0, tot_units * (1.0 + demand_pct_change))
            new_effective_price = avg_price_realized * (1.0 + price_shift_pct / 100.0)
            simulated_revenue = simulated_units * new_effective_price
            rev_delta = simulated_revenue - tot_rev
            rev_delta_pct = (rev_delta / max(1, tot_rev)) * 100.0

            st.markdown(f"""
            <div class="bi-sim-box">
                <div style="font-size: 11px; text-transform: uppercase; color: {cfg['text_secondary']}; font-weight: 700; margin-bottom: 6px;">
                    Projected Commercial Outcome
                </div>
                <div style="display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 10px;">
                    <span style="font-size: 24px; font-weight: 800; color: {cfg['accent_primary']};">
                        £{simulated_revenue:,.0f}
                    </span>
                    <span style="font-size: 13px; font-weight: 700; color: {'#10B981' if rev_delta >= 0 else '#F43F5E'};">
                        {'+' if rev_delta >= 0 else ''}£{rev_delta:,.0f} ({'+' if rev_delta >= 0 else ''}{rev_delta_pct:.1f}%)
                    </span>
                </div>
                <div style="font-size: 11px; color: {cfg['text_secondary']}; line-height: 1.6;">
                    • <b>Projected Demand:</b> {simulated_units:,.0f} units ({'+' if demand_pct_change >= 0 else ''}{demand_pct_change*100:.1f}%)<br>
                    • <b>Effective Realized Price:</b> £{new_effective_price:.2f}/unit<br>
                    • <b>Demand Sensitivity Model:</b> ε={elasticity:.1f} with {marketing_uplift}% promotional stimulus
                </div>
            </div>
            """, unsafe_allow_html=True)


# ===============================================================================
# TAB 6: END-TO-END PIPELINE DASHBOARD (PySpark, HDFS, & ML Evaluation)
# ===============================================================================
def render_pipeline_dashboard(pipeline_dir: Path, is_dark: bool = False):
    """
    Renders an executive, interactive Tableau/Power BI style interface
    for the End-to-End PySpark + HDFS Demand Forecasting Pipeline.
    """
    cfg = THEME_CONFIG["dark" if is_dark else "light"]

    summary_file = pipeline_dir / "pipeline_summary.json"
    forecasts_file = pipeline_dir / "product_forecasts.csv"
    segment_file = pipeline_dir / "validation_demand_segments.csv"
    product_validation_file = pipeline_dir / "validation_top_products.csv"

    if not (summary_file.exists() and forecasts_file.exists()):
        st.info(
            "Pipeline output is not present yet. Start the full local stack with `docker compose up --build`. "
            "The run downloads the UCI source dataset, executes PySpark ML pipelines, and writes Parquet to HDFS."
        )
        return

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    pipeline_forecasts = pd.read_csv(forecasts_file)
    evaluation = summary.get("evaluation", {})

    # 1. Pipeline Architecture Stepper (Visual Process Ribbon)
    st.markdown(f"""
    <div style="background: {cfg['bg_surface']}; border: 1px solid {cfg['border']}; border-radius: 8px; padding: 16px; margin-bottom: 18px; box-shadow: 0 2px 8px rgba(0,0,0,0.03);">
        <div style="font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: {cfg['text_secondary']}; margin-bottom: 12px;">
            Distributed Data Engineering & ML Architecture Workflow
        </div>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px;">
            <div style="background: {cfg['bg_card_hover']}; padding: 12px; border-radius: 6px; border-left: 3px solid #0078D4;">
                <div style="font-size: 10px; font-weight: 700; color: #0078D4; text-transform: uppercase;">1. Ingestion</div>
                <div style="font-weight: 700; font-size: 14px; color: {cfg['text_primary']};">{summary.get('source_rows_read_by_spark', 477096):,} Rows</div>
                <div style="font-size: 11px; color: {cfg['text_muted']};">Raw UCI Retail Feed</div>
            </div>
            <div style="background: {cfg['bg_card_hover']}; padding: 12px; border-radius: 6px; border-left: 3px solid #107C41;">
                <div style="font-size: 10px; font-weight: 700; color: #107C41; text-transform: uppercase;">2. Spark Hygiene</div>
                <div style="font-weight: 700; font-size: 14px; color: {cfg['text_primary']};">{summary.get('clean_rows_after_spark', 472251):,} Valid</div>
                <div style="font-size: 11px; color: {cfg['text_muted']};">Cancels & Negatives Filtered</div>
            </div>
            <div style="background: {cfg['bg_card_hover']}; padding: 12px; border-radius: 6px; border-left: 3px solid #D83B01;">
                <div style="font-size: 10px; font-weight: 700; color: #D83B01; text-transform: uppercase;">3. Feature Matrix</div>
                <div style="font-weight: 700; font-size: 14px; color: {cfg['text_primary']};">{summary.get('feature_rows', 1072632):,} Vectors</div>
                <div style="font-size: 11px; color: {cfg['text_muted']};">Lags (1, 7, 28) & Rolling Stats</div>
            </div>
            <div style="background: {cfg['bg_card_hover']}; padding: 12px; border-radius: 6px; border-left: 3px solid #5C2D91;">
                <div style="font-size: 10px; font-weight: 700; color: #5C2D91; text-transform: uppercase;">4. 30-Day Holdout</div>
                <div style="font-weight: 700; font-size: 14px; color: {cfg['text_primary']};">{int(evaluation.get('validation_rows', 88908)):,} Validated</div>
                <div style="font-size: 11px; color: {cfg['text_muted']};">{summary.get('validation_start')} → {summary.get('validation_end')}</div>
            </div>
            <div style="background: {cfg['bg_card_hover']}; padding: 12px; border-radius: 6px; border-left: 3px solid #00E5FF;">
                <div style="font-size: 10px; font-weight: 700; color: #0284C7; text-transform: uppercase;">5. Production Lake</div>
                <div style="font-weight: 700; font-size: 14px; color: {cfg['text_primary']};">{len(pipeline_forecasts):,} SKUs</div>
                <div style="font-size: 11px; color: {cfg['text_muted']};">HDFS Parquet Storage</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 2. Interactive Metric Switcher & KPI Scorecards
    ctrl_col, _ = st.columns([2, 3])
    with ctrl_col:
        metric_choice = st.radio(
            "Evaluation Metric Focus",
            options=["Mean Absolute Error (MAE)", "Root Mean Squared Error (RMSE)"],
            horizontal=True,
            help="Toggle between linear scale error (MAE) and outlier-penalizing error (RMSE)"
        )
    is_mae = "MAE" in metric_choice

    ml_score = evaluation.get("ml_mae" if is_mae else "ml_rmse", 8.97)
    base_score = evaluation.get("baseline_mae" if is_mae else "baseline_rmse", 9.43)
    weekday_score = evaluation.get("four_week_weekday_mae" if is_mae else "four_week_weekday_rmse", 9.28)

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: #0078D4;">
            <div class="bi-kpi-title">
                <span>Machine Learning Model</span>
                <span style="color: #107C41; font-weight: 700;">★ Best</span>
            </div>
            <div class="bi-kpi-value">{ml_score:.2f} <span style="font-size: 14px; font-weight: 500; color: {cfg['text_secondary']};">units</span></div>
            <div class="bi-kpi-delta bi-delta-pos">
                <span>Lowest Overall Error</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with kpi2:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: #4E79A7;">
            <div class="bi-kpi-title">
                <span>Recent 7-Day Moving Avg</span>
                <span>Baseline 1</span>
            </div>
            <div class="bi-kpi-value">{base_score:.2f} <span style="font-size: 14px; font-weight: 500; color: {cfg['text_secondary']};">units</span></div>
            <div class="bi-kpi-delta bi-delta-neu">
                <span>+{((base_score - ml_score) / ml_score) * 100:.1f}% vs ML</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with kpi3:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: #5C2D91;">
            <div class="bi-kpi-title">
                <span>4-Week Weekday Seasonal</span>
                <span>Baseline 2</span>
            </div>
            <div class="bi-kpi-value">{weekday_score:.2f} <span style="font-size: 14px; font-weight: 500; color: {cfg['text_secondary']};">units</span></div>
            <div class="bi-kpi-delta bi-delta-neu">
                <span>+{((weekday_score - ml_score) / ml_score) * 100:.1f}% vs ML</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with kpi4:
        st.markdown(f"""
        <div class="bi-kpi-card" style="border-top-color: #107C41;">
            <div class="bi-kpi-title">
                <span>Validation Coverage</span>
                <span>Holdout</span>
            </div>
            <div class="bi-kpi-value">{int(evaluation.get('validation_rows', 88908)):,}</div>
            <div class="bi-kpi-delta bi-delta-pos">
                <span>30 Full Calendar Days</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ===============================================================================
    # ROW 1: Validation Errors by Demand Band & 7-Day Forecast Comparison
    # ===============================================================================
    col_band, col_forecast = st.columns([1.2, 1.4])

    with col_band:
        if segment_file.exists():
            df_seg = pd.read_csv(segment_file)
            order_bands = ["0 units", "1-5 units", "6-20 units", "21-100 units", "Over 100 units"]
            df_seg["actual_demand_band"] = pd.Categorical(df_seg["actual_demand_band"], categories=order_bands, ordered=True)
            df_seg = df_seg.sort_values("actual_demand_band")

            fig_seg = go.Figure()
            fig_seg.add_trace(go.Bar(
                x=df_seg["actual_demand_band"],
                y=df_seg["ml_mae"],
                name="ML Model MAE",
                marker=dict(color="#0078D4"),
                hovertemplate="<b>%{x}</b><br>ML MAE: %{y:.2f} units<extra></extra>"
            ))
            fig_seg.add_trace(go.Bar(
                x=df_seg["actual_demand_band"],
                y=df_seg["rolling_7d_mae"],
                name="Rolling 7-Day MAE",
                marker=dict(color="#F28E2B"),
                hovertemplate="<b>%{x}</b><br>Rolling 7d MAE: %{y:.2f} units<extra></extra>"
            ))
            fig_seg.add_trace(go.Bar(
                x=df_seg["actual_demand_band"],
                y=df_seg["four_week_weekday_mae"],
                name="4-Wk Weekday MAE",
                marker=dict(color="#5C2D91"),
                hovertemplate="<b>%{x}</b><br>Weekday MAE: %{y:.2f} units<extra></extra>"
            ))

            layout_seg = build_plotly_layout(
                cfg,
                title="🎯 Validation Errors Segmented by Actual Daily Demand Band",
                height=380,
                is_dark=is_dark
            )
            layout_seg["barmode"] = "group"
            layout_seg["yaxis"]["title"] = "MAE (Units)"
            layout_seg["xaxis"]["title"] = "Daily Demand Band"
            fig_seg.update_layout(layout_seg)
            st.plotly_chart(fig_seg, use_container_width=True)

    with col_forecast:
        # Interactive Top N Selector & Comparator
        f_top_col1, f_top_col2 = st.columns([2, 1])
        with f_top_col1:
            st.markdown("""
            <div style="font-size: 14px; font-weight: 700; color: #0F172A; margin-top: 4px;">
                📈 7-Day Demand Forecast vs Baseline Comparator
            </div>
            """, unsafe_allow_html=True)
        with f_top_col2:
            top_n = st.selectbox("Display Top N Products", options=[10, 15, 25, 40], index=0)

        leaders = pipeline_forecasts.nlargest(top_n, "forecast_next_7d_units").copy()
        leaders["display_label"] = leaders["product_name"].str.slice(0, 18) + " (" + leaders["product_id"].astype(str) + ")"

        fig_fc = go.Figure()
        fig_fc.add_trace(go.Bar(
            x=leaders["display_label"],
            y=leaders["forecast_next_7d_units"],
            name="ML Model (7-Day Projection)",
            marker=dict(color="#0078D4", opacity=0.9),
            hovertemplate="<b>%{x}</b><br>ML Projection: %{y:,} units<extra></extra>"
        ))
        fig_fc.add_trace(go.Bar(
            x=leaders["display_label"],
            y=leaders["baseline_next_7d_units"],
            name="Recent 7-Day Average",
            marker=dict(color="#F28E2B", opacity=0.85),
            hovertemplate="<b>%{x}</b><br>Baseline: %{y:,} units<extra></extra>"
        ))
        fig_fc.add_trace(go.Scatter(
            x=leaders["display_label"],
            y=leaders["demand_buffer_units"],
            name="Safety Demand Buffer",
            mode="lines+markers",
            line=dict(color="#107C41", width=2.5, dash="dot"),
            marker=dict(size=6, color="#107C41"),
            hovertemplate="<b>%{x}</b><br>Buffer: %{y:,} units<extra></extra>"
        ))

        layout_fc = build_plotly_layout(
            cfg,
            title="",
            height=345,
            is_dark=is_dark
        )
        layout_fc["barmode"] = "group"
        layout_fc["xaxis"]["tickangle"] = -35
        layout_fc["yaxis"]["title"] = "Projected Units (7 Days)"
        fig_fc.update_layout(layout_fc)
        st.plotly_chart(fig_fc, use_container_width=True)

    # ===============================================================================
    # ROW 2: Interactive Safety Stock & Reorder Point Simulator
    # ===============================================================================
    st.markdown("""
    <div style="background: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 8px; padding: 18px; margin-bottom: 18px; box-shadow: 0 2px 8px rgba(0,0,0,0.03);">
        <div style="font-size: 14px; font-weight: 700; color: #0F172A; margin-bottom: 4px;">
            🎛️ Interactive Safety Stock & Reorder Point (ROP) Simulator
        </div>
        <div style="font-size: 12px; color: #64748B; margin-bottom: 14px;">
            Simulate supplier lead times and safety buffer factors to calculate inventory replenishment thresholds in real-time.
        </div>
    </div>
    """, unsafe_allow_html=True)

    sim_row1, sim_row2 = st.columns([1.2, 1.4])
    catalog_skus = pipeline_forecasts.nlargest(100, "forecast_next_7d_units")
    sku_options = {
        row["product_id"]: f"{row['product_id']} — {row['product_name']}"
        for _, row in catalog_skus.iterrows()
    }

    with sim_row1:
        sel_sku = st.selectbox(
            "Select Catalog SKU for Simulation",
            options=list(sku_options.keys()),
            format_func=lambda s: sku_options[s]
        )
        lead_time_days = st.slider("Supplier Replenishment Lead Time (Days)", min_value=1, max_value=28, value=7, step=1)
        safety_multiplier = st.slider("Safety Stock Buffer Multiplier (α)", min_value=0.5, max_value=2.5, value=1.0, step=0.1)

    sku_record = pipeline_forecasts[pipeline_forecasts["product_id"] == sel_sku].iloc[0]
    daily_forecast = float(sku_record["forecast_next_day_units"])
    base_buffer = float(sku_record["demand_buffer_units"])
    calc_lead_time_demand = daily_forecast * lead_time_days
    calc_safety_stock = base_buffer * safety_multiplier
    calc_reorder_point = int(np.ceil(calc_lead_time_demand + calc_safety_stock))

    with sim_row2:
        st.markdown(f"""
        <div style="background: #F8FAFC; border: 1px solid #E2E8F0; border-left: 4px solid #0078D4; border-radius: 8px; padding: 16px;">
            <div style="font-size: 11px; font-weight: 700; text-transform: uppercase; color: #64748B; margin-bottom: 8px;">
                Calculated Inventory Thresholds: {sku_record['product_name']}
            </div>
            <div style="display: flex; gap: 24px; align-items: baseline; margin-bottom: 12px;">
                <div>
                    <div style="font-size: 11px; color: #64748B;">Reorder Point (ROP) Trigger</div>
                    <div style="font-size: 26px; font-weight: 800; color: #0078D4;">{calc_reorder_point:,} <span style="font-size: 13px; font-weight: 600;">units</span></div>
                </div>
                <div>
                    <div style="font-size: 11px; color: #64748B;">Safety Buffer Reserve</div>
                    <div style="font-size: 22px; font-weight: 700; color: #107C41;">{int(calc_safety_stock):,} <span style="font-size: 13px; font-weight: 600;">units</span></div>
                </div>
                <div>
                    <div style="font-size: 11px; color: #64748B;">Lead-Time Consumption</div>
                    <div style="font-size: 22px; font-weight: 700; color: #D83B01;">{int(calc_lead_time_demand):,} <span style="font-size: 13px; font-weight: 600;">units</span></div>
                </div>
            </div>
            <div style="font-size: 12px; color: #475569; line-height: 1.6; border-top: 1px solid #E2E8F0; padding-top: 8px;">
                • <b>Daily Demand Rate:</b> {daily_forecast:.1f} units/day (Observed Median Price: £{sku_record.get('median_unit_price', 2.50):.2f})<br>
                • <b>Policy Formula:</b> <code>ROP = (Daily Demand × Lead Time) + (Safety Buffer × α)</code><br>
                • <b>Replenishment Rule:</b> Place purchase order as soon as effective stock on hand drops to or below <b>{calc_reorder_point:,} units</b>.
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ===============================================================================
    # ROW 3: Top 20 Validation Products Table & HDFS Specs
    # ===============================================================================
    st.markdown("""
    <div style="font-size: 14px; font-weight: 700; color: #0F172A; margin-top: 10px; margin-bottom: 4px;">
        📋 Held-Out Validation Performance for 20 Highest-Volume Products
    </div>
    <div style="font-size: 12px; color: #64748B; margin-bottom: 10px;">
        Totals reflect the 30-day evaluation holdout. MAE measures average daily unit error per SKU.
    </div>
    """, unsafe_allow_html=True)

    if product_validation_file.exists():
        df_prod_val = pd.read_csv(product_validation_file)
        st.dataframe(
            df_prod_val,
            column_config={
                "product_id": st.column_config.TextColumn("SKU ID", width="small"),
                "product_name": st.column_config.TextColumn("Product Description", width="large"),
                "actual_units": st.column_config.ProgressColumn(
                    "Actual Units Sold",
                    format="%d",
                    min_value=0,
                    max_value=float(df_prod_val["actual_units"].max()),
                ),
                "ml_predicted_units": st.column_config.NumberColumn("ML Predicted", format="%d"),
                "weekday_baseline_units": st.column_config.NumberColumn("Weekday Baseline", format="%d"),
                "ml_mae": st.column_config.NumberColumn("ML MAE", format="%.2f"),
                "weekday_baseline_mae": st.column_config.NumberColumn("Baseline MAE", format="%.2f"),
            },
            use_container_width=True,
            hide_index=True,
            height=320,
        )

    with st.expander("Distributed Lake & HDFS Partition Storage Specs"):
        st.write("**HDFS Cluster Root:**", summary.get("hdfs_root", "hdfs://namenode:8020/market_demand"))
        st.json(summary.get("outputs", {}))
        st.caption(
            "The cluster runs Apache Hadoop NameNode and dual DataNode processes with replication factor 2. "
            "Cleaned datasets and ML feature partitions are persisted as distributed Parquet format."
        )

