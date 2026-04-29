import awswrangler as wr
import pandas as pd
import boto3
from datetime import timedelta

# Configure default AWS session region so awswrangler knows where Athena lives
boto3.setup_default_session(region_name="us-east-1")

DATABASE = "retail_analytics_dev"
S3_STAGING_DIR = "s3://retail-legacy-dev-athena-results-mykhailo-2026/"

def run_athena_query(sql: str) -> pd.DataFrame:
    """Executes a SQL query in AWS Athena and returns a Pandas DataFrame."""
    df = wr.athena.read_sql_query(sql=sql, database=DATABASE, s3_output=S3_STAGING_DIR)
    
    # Athena automatically converts all column names to lowercase.
    # We map them back to the exact casing expected by the Streamlit app.
    casing_map = {
        "date": "DATE",
        "current_sales": "CURRENT_SALES",
        "prev_sales": "PREV_SALES",
        "prev_yoy_sales": "PREV_YOY_SALES",
        "wow_pct_increase": "WOW_PCT_INCREASE",
        "yoy_pct_increase": "YOY_PCT_INCREASE",
        "prev_week_sales": "PREV_WEEK_SALES",
        "promo_sales": "PROMO_SALES",
        "prev_year_sales": "PREV_YEAR_SALES",
        "period_wow_pct": "Period_WOW_PCT",
        "period_yoy_pct": "Period_YOY_PCT",
        "promo_penetration_pct": "Promo_Penetration_PCT",
        "seg": "SEG",
        "actual": "ACTUAL",
        "promo": "PROMO",
        "product_category": "PRODUCT_CATEGORY",
        "opstudy_label": "OPSTUDY_LABEL",
        "pln_label": "PLN_LABEL",
        "bu": "BU"
    }
    return df.rename(columns=casing_map)

def get_filters() -> tuple:
    sql = f"""
        SELECT 
            MIN(d.full_date) AS min_date, 
            MAX(d.full_date) AS max_date 
        FROM dim_date d
        JOIN fact_sales f ON d.date_key = f.date_key
    """
    df = run_athena_query(sql)
    return pd.to_datetime(df['min_date'].iloc[0]).date(), pd.to_datetime(df['max_date'].iloc[0]).date()

def get_weekly_metrics(start_date, end_date) -> pd.DataFrame:
    sql = f"""
    WITH weekly_sales AS (
        SELECT 
            d.full_date AS DATE,
            SUM(f.actual_sales) AS CURRENT_SALES
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.full_date
    ),
    lagged_sales AS (
        SELECT 
            DATE,
            CURRENT_SALES,
            LAG(CURRENT_SALES, 1) OVER (ORDER BY DATE) AS PREV_SALES,
            LAG(CURRENT_SALES, 52) OVER (ORDER BY DATE) AS PREV_YOY_SALES
        FROM weekly_sales
    )
    SELECT 
        DATE,
        CURRENT_SALES,
        PREV_SALES,
        PREV_YOY_SALES,
        CASE 
            WHEN PREV_SALES IS NULL OR PREV_SALES = 0 THEN 0.0
            ELSE ((CURRENT_SALES - PREV_SALES) / PREV_SALES) * 100.0 
        END AS WOW_PCT_INCREASE,
        CASE 
            WHEN PREV_YOY_SALES IS NULL OR PREV_YOY_SALES = 0 THEN 0.0
            ELSE ((CURRENT_SALES - PREV_YOY_SALES) / PREV_YOY_SALES) * 100.0 
        END AS YOY_PCT_INCREASE
    FROM lagged_sales
    WHERE DATE BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    ORDER BY DATE
    """
    return run_athena_query(sql)

def get_top_n_metrics(start_date, end_date, group_col: str, top_n: int, exclude_values: list[str] = None) -> pd.DataFrame:
    col_map = {
        "PRODUCT_CATEGORY": "p.broad_category",
        "OPSTUDY_LABEL": "p.opstudy_label",
        "PLN_LABEL": "p.pln_label",
        "BU": "p.business_unit"
    }
    db_col = col_map.get(group_col, group_col)
    
    exclude_clause = ""
    if exclude_values:
        ex_vals = ", ".join([f"'{v}'" for v in exclude_values])
        exclude_clause = f"AND {db_col} NOT IN ({ex_vals})"

    sql = f"""
    WITH base_weekly_agg AS (
        SELECT 
            d.full_date AS DATE,
            {db_col} AS {group_col},
            SUM(f.actual_sales) AS CURRENT_SALES,
            SUM(CASE WHEN f.is_promo THEN f.actual_sales ELSE 0 END) AS PROMO_SALES
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_product p ON f.pln_id = p.pln_id
        WHERE 1=1 {exclude_clause}
        GROUP BY d.full_date, {db_col}
    ),
    lagged AS (
        SELECT 
            DATE,
            {group_col},
            CURRENT_SALES,
            PROMO_SALES,
            LAG(CURRENT_SALES, 1) OVER (PARTITION BY {group_col} ORDER BY DATE) AS PREV_WEEK_SALES,
            LAG(CURRENT_SALES, 52) OVER (PARTITION BY {group_col} ORDER BY DATE) AS PREV_YEAR_SALES
        FROM base_weekly_agg
    ),
    filtered_agg AS (
        SELECT 
            {group_col},
            SUM(CURRENT_SALES) AS CURRENT_SALES,
            SUM(PREV_WEEK_SALES) AS PREV_WEEK_SALES,
            SUM(PROMO_SALES) AS PROMO_SALES,
            SUM(PREV_YEAR_SALES) AS PREV_YEAR_SALES
        FROM lagged
        WHERE DATE BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY {group_col}
    )
    SELECT 
        {group_col},
        CURRENT_SALES,
        COALESCE(PREV_WEEK_SALES, 0.0) AS PREV_WEEK_SALES,
        COALESCE(PROMO_SALES, 0.0) AS PROMO_SALES,
        COALESCE(PREV_YEAR_SALES, 0.0) AS PREV_YEAR_SALES,
        CASE WHEN PREV_WEEK_SALES IS NULL OR PREV_WEEK_SALES = 0 THEN 0.0
             ELSE ((CURRENT_SALES - PREV_WEEK_SALES) / PREV_WEEK_SALES) * 100.0 END AS Period_WOW_PCT,
        CASE WHEN PREV_YEAR_SALES IS NULL OR PREV_YEAR_SALES = 0 THEN 0.0
             ELSE ((CURRENT_SALES - PREV_YEAR_SALES) / PREV_YEAR_SALES) * 100.0 END AS Period_YOY_PCT,
        CASE WHEN CURRENT_SALES = 0 THEN 0.0
             ELSE (PROMO_SALES / CURRENT_SALES) * 100.0 END AS Promo_Penetration_PCT
    FROM filtered_agg
    ORDER BY CURRENT_SALES DESC
    LIMIT {top_n}
    """
    df = run_athena_query(sql)
    return df.sort_values("CURRENT_SALES", ascending=True)

def get_segment_metrics(start_date, end_date) -> pd.DataFrame:
    sql = f"""
    SELECT 
        s.segment_id AS SEG,
        SUM(f.actual_sales) AS ACTUAL
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_segment s ON f.segment_id = s.segment_id
    WHERE d.full_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    GROUP BY s.segment_id
    """
    return run_athena_query(sql)

def get_promo_metrics(start_date, end_date) -> pd.DataFrame:
    sql = f"""
    SELECT 
        d.full_date AS DATE,
        CASE WHEN f.is_promo THEN 'Y' ELSE 'N' END AS PROMO,
        SUM(f.actual_sales) AS ACTUAL
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    GROUP BY d.full_date, f.is_promo
    ORDER BY DATE
    """
    return run_athena_query(sql)

def get_promo_lift_metrics(start_date, end_date) -> pd.DataFrame:
    sql = f"""
    SELECT 
        p.broad_category AS PRODUCT_CATEGORY,
        CASE WHEN f.is_promo THEN 'Y' ELSE 'N' END AS PROMO,
        SUM(f.actual_sales) AS ACTUAL
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_product p ON f.pln_id = p.pln_id
    WHERE d.full_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    GROUP BY p.broad_category, f.is_promo
    """
    return run_athena_query(sql)

def get_category_mix_metrics(start_date, end_date) -> pd.DataFrame:
    sql = f"""
    SELECT 
        d.full_date AS DATE,
        p.broad_category AS PRODUCT_CATEGORY,
        SUM(f.actual_sales) AS ACTUAL
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_product p ON f.pln_id = p.pln_id
    WHERE d.full_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
      AND p.broad_category NOT IN ('Other', 'Unknown', 'Uncategorized')
    GROUP BY d.full_date, p.broad_category
    ORDER BY DATE
    """
    return run_athena_query(sql)