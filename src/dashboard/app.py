import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import timedelta

# Add the project root to the Python path to resolve 'src' module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import queries

st.set_page_config(page_title="Retail Sales Dashboard", layout="wide")

# ---- Cached Data Loading & Aggregation ----
@st.cache_data(ttl=3600)
def get_filters() -> tuple:
    """Efficiently scan the dataset to get sidebar bounds without loading data into memory."""
    return queries.get_filters()

@st.cache_data(ttl=3600)
def get_weekly_metrics(start_date, end_date) -> pd.DataFrame:
    """Calculates overall weekly KPIs dynamically using AWS Athena."""
    return queries.get_weekly_metrics(start_date, end_date)

@st.cache_data(ttl=3600)
def get_top_n_metrics(start_date, end_date, group_col: str, top_n: int, exclude_values: list[str] = None) -> pd.DataFrame:
    """Calculates top N groups with WoW, YoY, and Promo metrics dynamically using AWS Athena."""
    return queries.get_top_n_metrics(start_date, end_date, group_col, top_n, exclude_values)

@st.cache_data(ttl=3600)
def get_segment_metrics(start_date, end_date) -> pd.DataFrame:
    """Calculates revenue by ABC inventory segment."""
    return queries.get_segment_metrics(start_date, end_date)

@st.cache_data(ttl=3600)
def get_promo_metrics(start_date, end_date) -> pd.DataFrame:
    """Calculates weekly sales by promotion status."""
    return queries.get_promo_metrics(start_date, end_date)

@st.cache_data(ttl=3600)
def get_promo_lift_metrics(start_date, end_date) -> pd.DataFrame:
    """Calculates promotion lift by product category."""
    return queries.get_promo_lift_metrics(start_date, end_date)

@st.cache_data(ttl=3600)
def get_category_mix_metrics(start_date, end_date) -> pd.DataFrame:
    """Calculates weekly sales by product category for stacked area chart."""
    return queries.get_category_mix_metrics(start_date, end_date)

# ---- Layout & Presentation ----
st.title("Retail Sales Dashboard")

# ---- Horizontal Filters ----
min_date, max_date = get_filters()

def restore_dates_callback():
    st.session_state.date_range_picker = [min_date, max_date]

filter_col1, filter_col2, _ = st.columns([2, 1, 5])

with filter_col1:
    date_range = st.date_input(
        "Select Date Range",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date,
        key="date_range_picker"
    )

with filter_col2:
    st.write("") # Vertical alignment spacing
    st.write("")
    st.button("Restore Default Dates", on_click=restore_dates_callback)

# Handle date_input gracefully if user selects just 1 date instead of a range
if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = date_range[0], date_range[0]

st.divider()

# ---- Execute Lazily and Cache ----
df_metrics = get_weekly_metrics(start_date, end_date)
df_segments = get_segment_metrics(start_date, end_date)
df_promo = get_promo_metrics(start_date, end_date)
df_promo_lift = get_promo_lift_metrics(start_date, end_date)
df_category_mix = get_category_mix_metrics(start_date, end_date)

total_sales_kpi = df_metrics["CURRENT_SALES"].sum() if not df_metrics.empty else 0

if not df_metrics.empty:
    max_filtered_date = df_metrics["DATE"].max()
    latest_wow = df_metrics.loc[df_metrics["DATE"] == max_filtered_date, "WOW_PCT_INCREASE"].iloc[0]
    latest_yoy = df_metrics.loc[df_metrics["DATE"] == max_filtered_date, "YOY_PCT_INCREASE"].iloc[0]
else:
    latest_wow, latest_yoy = 0.0, 0.0

kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
with kpi_col1:
    st.metric("Total Sales (Period)", f"{total_sales_kpi:,.0f}")
with kpi_col2:
    st.metric("Latest Week WoW", f"{latest_wow:,.2f}%", delta=f"{latest_wow:,.2f}%")
with kpi_col3:
    st.metric("Latest Week YoY", f"{latest_yoy:,.2f}%", delta=f"{latest_yoy:,.2f}%")

st.divider()

tab_sales, tab_marketing, tab_inventory = st.tabs(["Sales & Executives", "Marketing", "Inventory Management"])

# ---- Tab 1: Sales & Executives ----
with tab_sales:
    ts_col1, ts_col2 = st.columns(2)

    with ts_col1:
        granularity = st.radio(
            "Granularity",
            ["Weekly", "Monthly", "Yearly"],
            horizontal=True
        )
        if not df_metrics.empty:
            plot_df = df_metrics[["DATE", "CURRENT_SALES"]].copy()
            
            if granularity == "Monthly":
                plot_df["DATE"] = plot_df["DATE"].dt.to_period("M").dt.to_timestamp()
                plot_df = plot_df.groupby("DATE", as_index=False)["CURRENT_SALES"].sum()
            elif granularity == "Yearly":
                plot_df["DATE"] = plot_df["DATE"].dt.to_period("Y").dt.to_timestamp()
                plot_df = plot_df.groupby("DATE", as_index=False)["CURRENT_SALES"].sum()

            fig_line = px.area(
                plot_df.sort_values("DATE"), 
                x="DATE", 
                y="CURRENT_SALES",
                markers=True,
                color_discrete_sequence=["#00b4d8"],
                labels={"CURRENT_SALES": "Total Sales", "DATE": "Date"},
                title=f"Total {granularity} Sales"
            )
            fig_line.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis=dict(showgrid=False),
                margin=dict(l=0, r=10, t=40, b=0)
            )
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("No data available for the selected period.")

    with ts_col2:
        growth_metric = st.radio(
            "Growth Metric",
            ["WoW %", "YoY %"],
            horizontal=True
        )
        if not df_metrics.empty:
            y_col = "WOW_PCT_INCREASE" if growth_metric == "WoW %" else "YOY_PCT_INCREASE"
            y_label = "WoW % Increase" if growth_metric == "WoW %" else "YoY % Increase"
            
            fig_growth = px.line(
                df_metrics.sort_values("DATE"), 
                x="DATE", 
                y=y_col,
                markers=True,
                color_discrete_sequence=["#ffb703"],
                labels={y_col: y_label, "DATE": "Week"},
                title=f"Weekly {growth_metric} Trend"
            )
            fig_growth.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis=dict(showgrid=False),
                margin=dict(l=0, r=10, t=40, b=0)
            )
            fig_growth.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig_growth, use_container_width=True)
        else:
            st.info("No data available for the selected period.")

    st.divider()

    if not df_category_mix.empty:
        st.subheader("Category Mix Over Time")
        fig_mix = px.area(
            df_category_mix.sort_values("DATE"),
            x="DATE",
            y="ACTUAL",
            color="PRODUCT_CATEGORY",
            groupnorm='percent',
            title="Weekly Sales Contribution by Product Category (%)",
            labels={"ACTUAL": "Sales Contribution (%)", "DATE": "Date"},
            color_discrete_sequence=px.colors.qualitative.Vivid
        )
        fig_mix.update_layout(
            yaxis_ticksuffix="%",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig_mix, use_container_width=True)

# ---- Tab 2: Marketing ----
with tab_marketing:
    mkt_col1, mkt_col2 = st.columns(2)
    
    with mkt_col1:
        promo_ctrl1, promo_ctrl2 = st.columns(2)
        with promo_ctrl1:
            promo_view_type = st.radio(
                "Promotion Chart View",
                ["Percentage (%)","Volume"],
                horizontal=True,
                label_visibility="collapsed"
            )
        with promo_ctrl2:
            promo_granularity = st.radio(
                "Promotion Granularity",
                ["Weekly", "Yearly"],
                horizontal=True,
                label_visibility="collapsed"
            )

        if not df_promo.empty:
            y_label = f"Percentage of {promo_granularity} Sales" if promo_view_type == "Percentage (%)" else "Sales Volume"
            x_label = "Year" if promo_granularity == "Yearly" else "Week"

            plot_promo_df = df_promo.copy()
            if promo_granularity == "Yearly":
                plot_promo_df["DATE"] = plot_promo_df["DATE"].dt.to_period("Y").dt.to_timestamp()
                plot_promo_df = plot_promo_df.groupby(["DATE", "PROMO"], as_index=False)["ACTUAL"].sum()

            fig_promo_bar = px.bar(
                plot_promo_df.sort_values("DATE"),
                x='DATE',
                y='ACTUAL',
                color='PROMO',
                title=f"{promo_granularity} Sales Volume by Promotion Status",
                labels={"ACTUAL": y_label, "DATE": x_label, "PROMO": "Promo"},
                color_discrete_map={'Y': '#fb8500', 'N': '#023047'}
            )

            if promo_view_type == "Percentage (%)":
                fig_promo_bar.update_layout(barnorm='percent')

            st.plotly_chart(fig_promo_bar, use_container_width=True)
        else:
            st.info("No promotion data available for the selected period.")

    with mkt_col2:
        lift_view_type = st.radio(
            "Promo Lift View",
            ["Percentage (%)", "Volume"],
            horizontal=True,
            label_visibility="collapsed",
            key="lift_view"
        )

        st.write("") # Vertical spacing alignment
        st.subheader("Promo Lift by Category")
        if not df_promo_lift.empty:
            # Filter out 'Other', 'Unknown', and 'Uncategorized' from the visual ranking
            filtered_promo_lift = df_promo_lift[~df_promo_lift["PRODUCT_CATEGORY"].isin(["Other", "Unknown", "Uncategorized"])]
            
            y_label_lift = "Percentage (%)" if lift_view_type == "Percentage (%)" else "Sales Volume"
            barmode = "stack" if lift_view_type == "Percentage (%)" else "group"
            
            fig_promo_lift = px.bar(
                filtered_promo_lift.sort_values("ACTUAL", ascending=False),
                x="PRODUCT_CATEGORY",
                y="ACTUAL",
                color="PROMO",
                barmode=barmode,
                title="Promo Lift by Category",
                labels={"ACTUAL": y_label_lift, "PRODUCT_CATEGORY": "Category", "PROMO": "Promo"},
                color_discrete_map={'Y': '#fb8500', 'N': '#023047'}
            )

            if lift_view_type == "Percentage (%)":
                fig_promo_lift.update_layout(barnorm='percent')

            fig_promo_lift.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig_promo_lift, use_container_width=True)
        else:
            st.info("No promotion lift data available.")

# ---- Tab 3: Inventory Management ----
with tab_inventory:
    if not df_segments.empty:
        # Define a color map to highlight 'A' segment
        color_map = {'A': '#023e8a', 'B': '#0096c7', 'C': '#90e0ef'}
        fig_donut = px.pie(
            df_segments,
            names='SEG',
            values='ACTUAL',
            hole=0.5,
            color='SEG',
            color_discrete_map=color_map,
            title="Revenue by ABC Inventory Segment"
        )
        fig_donut.update_traces(textposition='inside', textinfo='percent+label')
        fig_donut.update_layout(
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=20, r=20, t=40, b=20)
        )
        st.plotly_chart(fig_donut, use_container_width=True)
    else:
        st.info("No segment data available for the selected period.")

    st.divider()
    st.header("Top Performers")

    ctrl_col1, ctrl_col2, ctrl_col3 = st.columns(3)
    with ctrl_col1:
        top_n = st.number_input("Top N Items", min_value=1, max_value=100, value=10)
    with ctrl_col2:
        m_weeks = st.slider("Last M Weeks (for Top N charts)", min_value=1, max_value=156, value=12)
    with ctrl_col3:
        color_metric_selection = st.radio(
            "Color Metric",
            ["WoW %", "YoY %", "Promo Penetration %"],
            key="top_n_color_metric"
        )

    # Calculate the strict M-weeks lookback window based on the chosen end_date
    top_n_start_date = end_date - timedelta(weeks=m_weeks)
    if top_n_start_date < min_date:
        top_n_start_date = min_date

    if color_metric_selection == "WoW %":
        color_col = "Period_WOW_PCT"
        color_metric_name = "WoW %"
    elif color_metric_selection == "YoY %":
        color_col = "Period_YOY_PCT"
        color_metric_name = "YoY %"
    else: # "Promo Penetration %"
        color_col = "Promo_Penetration_PCT"
        color_metric_name = "Promo Pen. %"

    st.subheader(f"Top {top_n} Performers (Last {m_weeks} Weeks)")
    chart_col1, chart_col2 = st.columns(2)

    def plot_top_n_chart(title, top_agg_df, y_col, y_label, color_col, color_metric_name):
        if not top_agg_df.empty:
            if color_metric_name in ["WoW %", "YoY %"]:
                color_scale = px.colors.diverging.RdYlGn
                midpoint = 0
            else:
                color_scale = px.colors.sequential.Purples
                midpoint = None

            fig = px.bar(top_agg_df, x="CURRENT_SALES", y=y_col, orientation="h", 
                         color=color_col,
                         color_continuous_scale=color_scale,
                         color_continuous_midpoint=midpoint,
                         text_auto=".2s",
                         hover_data={
                             "Period_WOW_PCT": ":.2f", 
                             "Period_YOY_PCT": ":.2f",
                             "Promo_Penetration_PCT": ":.2f",
                             "PREV_WEEK_SALES": ":.0f",
                             "PREV_YEAR_SALES": ":.0f",
                             "PROMO_SALES": ":.0f"
                         },
                         title=title,
                         labels={"CURRENT_SALES": "Sales", y_col: y_label, color_col: color_metric_name})
            fig.update_layout(
                coloraxis_showscale=True,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False),
                margin=dict(l=0, r=10, t=40, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available.")

    with chart_col1:
        exclude_list = ["Other", "Unknown", "Uncategorized"]
        plot_top_n_chart(f"Top {top_n} Product Categories", get_top_n_metrics(top_n_start_date, end_date, "PRODUCT_CATEGORY", top_n, exclude_values=exclude_list), "PRODUCT_CATEGORY", "Category", color_col, color_metric_name)
        plot_top_n_chart(f"Top {top_n} OpStudies", get_top_n_metrics(top_n_start_date, end_date, "OPSTUDY_LABEL", top_n), "OPSTUDY_LABEL", "OpStudy", color_col, color_metric_name)

    with chart_col2:
        plot_top_n_chart(f"Top {top_n} PLNs (SKUs)", get_top_n_metrics(top_n_start_date, end_date, "PLN_LABEL", top_n), "PLN_LABEL", "PLN (SKU)", color_col, color_metric_name)
        plot_top_n_chart(f"Top {top_n} Business Units", get_top_n_metrics(top_n_start_date, end_date, "BU", top_n), "BU", "Business Unit", color_col, color_metric_name)