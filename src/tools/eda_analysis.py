import os
import sys

# Add the project root to the Python path to resolve 'src' module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import polars as pl
import polars.selectors as cs
from src.config import settings
from src.pipeline import transform

def showcase_dirty_row_transformation():
    """Demonstrates exactly how a messy CSV row is transformed by the pipeline."""
    print("--- 🪄 Pipeline Transformation Showcase (Before & After) ---")
    
    # Mocking a highly corrupted row exactly as it appears in the raw CSV
    dirty_df = pl.DataFrame({
        "WEEK": ['"1231015"'],                     # Legacy date format with literal quotes
        "PLN": ["1001"],                           # SKU ID
        "PLN_LABEL": ['"  mEsSy ItEm NaMe  "'],    # Messy casing, quotes, and whitespace
        "PRODUCT_CATEGORY_LABEL": ['" cHoCoLaTe  "'], # Needs stripping
        "OPSTUDY_LABEL": [" NVLTY/GUM/MINT "],     # Ugly acronym needing polish
        "BU": ["none"],                            # Null alias that should become 'Unknown'
        "ACTUAL": [10.5],
        "SEG": ["NoSegment-A"],                    # Weird system prefix
        "PROMO,,": ['"Y",,']                       # Corrupted column name, quotes, and trailing commas
    })

    print("BEFORE (Raw CSV Mess):")
    with pl.Config(tbl_rows=1, tbl_cols=10):
        print(dirty_df)
        
    # Run the exact same functions used in data_pipeline.py
    cleaned_lazy = transform.clean_raw_data(dirty_df.lazy())
    processed_df = transform.process_valid_data(cleaned_lazy.collect())

    print("\nAFTER (Cleaned, Standardized, & Typed Silver Layer):")
    with pl.Config(tbl_rows=1, tbl_cols=10):
        print(processed_df)
    print("\nNotice: Date is parsed, strings are capitalized/stripped, nulls are standardized, acronyms polished, and types optimized!\n")

def run_eda():
    showcase_dirty_row_transformation()
    print("Starting EDA on Gold Layer (Star Schema)...\n")

    # 1. Rebuild flat view from Gold Parquet files for analysis
    fact_sales = pl.read_parquet(settings.GOLD_FACT_SALES)
    dim_date = pl.read_parquet(settings.GOLD_DIM_DATE)
    dim_product = pl.read_parquet(settings.GOLD_DIM_PRODUCT)
    dim_segment = pl.read_parquet(settings.GOLD_DIM_SEGMENT)

    df = (
        fact_sales
        .join(dim_date, on="date_key", how="left")
        .join(dim_product, on="pln_id", how="left")
        .join(dim_segment, on="segment_id", how="left")
        .rename({
            "full_date": "DATE", "pln_id": "PLN", "actual_sales": "ACTUAL",
            "segment_id": "SEG", "business_unit": "BU", "opstudy_label": "OPSTUDY_LABEL",
            "broad_category": "PRODUCT_CATEGORY", "pln_label": "PLN_LABEL"
        })
        .with_columns(pl.when(pl.col("is_promo")).then(pl.lit("Y")).otherwise(pl.lit("N")).alias("PROMO"))
    )

    # 3. Data Quality Checks
    print("--- Basic Information ---")
    print(f"Total Rows: {df.height:,}")
    print(f"Total Columns: {df.width}\n")

    print("--- Missing Values ---")
    print(df.null_count(), "\n")

    print("--- Duplicates ---")
    duplicate_count = df.is_duplicated().sum()
    print(f"Duplicate Rows: {duplicate_count:,}\n")

    print("--- 'ACTUAL' Sales Anomalies ---")
    zero_or_negative_sales = df.filter(pl.col("ACTUAL") <= 0).height
    print(f"Rows with ACTUAL <= 0 (Returns/Errors?): {zero_or_negative_sales:,}\n")

    print("--- Numeric Summary Statistics ---")
    print(df.select(cs.numeric()).describe(), "\n")

    # 4. Data Hierarchy & Cardinality Problems
    print("--- Cardinality Check ---")
    cardinality = df.select(
        pl.col("PLN").n_unique().alias("Unique PLNs (SKUs)"),
        pl.col("PRODUCT_CATEGORY").n_unique().alias("Unique Categories"),
        pl.col("OPSTUDY_LABEL").n_unique().alias("Unique OpStudies"),
        pl.col("SEG").n_unique().alias("Unique Segments"),
        pl.col("BU").n_unique().alias("Unique BUs")
    )
    print(cardinality, "\n")

    # 4.5. Categorization Success (Gold Layer Audit)
    print("--- Categorization Success (Gold Layer Audit) ---")
    category_distribution = (
        df.group_by("PRODUCT_CATEGORY")
        .agg(
            pl.len().alias("Row_Count"),
            pl.col("ACTUAL").sum().alias("Total_Sales")
        )
        .with_columns((pl.col("Total_Sales") / pl.col("Total_Sales").sum() * 100).alias("Sales_Pct"))
        .sort("Total_Sales", descending=True)
    )
    print("Sales Distribution by Category:")
    print(category_distribution, "\n")
    
    other_pct = category_distribution.filter(pl.col("PRODUCT_CATEGORY") == "Other").select("Sales_Pct")
    if not other_pct.is_empty():
        print(f"📊 'Other' accounts for {other_pct.item():.2f}% of total sales.\n")
        
        print("--- Visualizing Unmapped Raw Categories in 'Other' ---")
        unmapped_categories = (
            df.filter(pl.col("PRODUCT_CATEGORY") == "Other")
            .group_by("legacy_category_label")
            .agg(pl.col("ACTUAL").sum().alias("Total_Sales"))
            .sort("Total_Sales", descending=True)
            .head(30)
        )
        print("Top 30 Unmapped Raw Categories in 'Other':")
        with pl.Config(tbl_rows=30):
            print(unmapped_categories)
            
        print("\n--- Top 20 Revenue-Generating Products in 'Other' ---")
        top_other_products = (
            df.filter(pl.col("PRODUCT_CATEGORY") == "Other")
            .group_by("PLN_LABEL")
            .agg(pl.col("ACTUAL").sum().alias("Total_Sales"))
            .sort("Total_Sales", descending=True)
            .head(20)
        )
        with pl.Config(tbl_rows=20):
            print(top_other_products)
    else:
        print("✅ 'Other' category is empty or missing! Great job.\n")

    # 5. Time Series Continuity Check (Highlighting the Gap Problem)
    print("--- Time Series Gap Analysis ---")
    total_weeks = df.select(pl.col("DATE").n_unique()).item()
    print(f"Total unique dates in dataset: {total_weeks}")

    gaps_analysis = (
        df.group_by(["PLN", "SEG", "PROMO"])
        .agg(
            pl.col("DATE").n_unique().alias("Weeks_Present"),
            pl.col("DATE").min().alias("First_Seen"),
            pl.col("DATE").max().alias("Last_Seen")
        )
        .with_columns(
            ((pl.col("Last_Seen") - pl.col("First_Seen")).dt.total_days() / 7 + 1).cast(pl.Int64).alias("Expected_Weeks")
        )
        .with_columns(
            (pl.col("Expected_Weeks") - pl.col("Weeks_Present")).alias("Missing_Weeks")
        )
        .filter(pl.col("Missing_Weeks") > 0)
    )
    print(f"Number of (PLN, SEG, PROMO) combinations with missing weeks: {gaps_analysis.height:,}")
    if gaps_analysis.height > 0:
        print("Sample of combinations with the most missing weeks:")
        print(gaps_analysis.sort("Missing_Weeks", descending=True).head())

if __name__ == "__main__":
    run_eda()