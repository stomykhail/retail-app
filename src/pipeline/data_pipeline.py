import polars as pl
import os
from src.pipeline import transform
from src.config import settings

# Enable string cache for categorical mappings (Best practice for Parquet exports)
pl.enable_string_cache()

def map_category(label):
    if not label:
        return "Other"
    text = str(label).strip().upper()
    if text in settings.EXACT_MATCHES:
        return settings.EXACT_MATCHES[text]
    for category, keywords in settings.KEYWORD_MAPPINGS.items():
        if any(keyword in text for keyword in keywords):
            return category
    return "Other"

def load_and_clean_data(file_path: str) -> pl.LazyFrame:
    """
    Loads the raw CSV and applies the data cleaning plan.
    Returns a single LazyFrame representing the cleaned and tagged data.
    """
    # 1. Load Data
    lazy_df = pl.scan_csv(file_path, separator="|", ignore_errors=True, quote_char=None)
    return transform.clean_raw_data(lazy_df)

def run_etl(input_file: str = None):
    """Main execution function to run the Medallion ETL pipeline."""
    if input_file is None:
        input_file = settings.RAW_DATA_PATH
        
    os.makedirs(os.path.dirname(settings.SILVER_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(settings.GOLD_FACT_SALES), exist_ok=True)
    os.makedirs(os.path.dirname(settings.QUARANTINE_FILE), exist_ok=True)

    print(f"Running data pipeline for {input_file}...")
    
    # 1. Read CSV ONCE and tag data lazily
    base_lazy_df = load_and_clean_data(input_file)
    
    # 2. Collect the tagged dataset ONCE (Prevents reading the CSV twice)
    print("Parsing CSV and applying initial text/date cleans (this may take a moment)...")
    base_df = base_lazy_df.collect()
    initial_row_count = base_df.height
    print(f"📊 Total Raw Rows Parsed: {initial_row_count:,}")
    
    # 3. Split and Save Quarantine Layer
    print("Splitting Quarantine data...")
    quarantine_df = base_df.filter(pl.col("ERROR_REASON") != "Valid")
    quarantine_row_count = quarantine_df.height
    quarantine_df.write_parquet(settings.QUARANTINE_FILE)
    print(f"Saved Quarantine Layer to: {settings.QUARANTINE_FILE} ({quarantine_row_count:,} rows)")
    
    print("🔍 Quarantine Breakdown:")
    reason_counts = quarantine_df.group_by("ERROR_REASON").len().sort("len", descending=True)
    for row in reason_counts.iter_rows():
        print(f"   - {row[0]}: {row[1]:,}")

    # 4. Process Valid Data and Save Silver Layer
    print("Processing Golden Records and saving Silver Layer...")
    silver_df = transform.process_valid_data(base_df)
    silver_row_count = silver_df.height
    silver_df.write_parquet(settings.SILVER_FILE)
    dropped_duplicates = (initial_row_count - quarantine_row_count) - silver_row_count
    print(f"Saved Silver Layer to: {settings.SILVER_FILE} ({silver_row_count:,} rows)")
    print(f"Drop Analytics: {quarantine_row_count:,} invalid rows quarantined | {dropped_duplicates:,} duplicate rows dropped.")
    
    # 5. Generate Gold Layer (Star Schema)
    print("Generating Gold Layer (Star Schema)...")
    silver_lazy = silver_df.lazy()
    
    print("  -> Resolving category mappings in memory...")
    unique_categories = silver_df.select(pl.col("PRODUCT_CATEGORY_LABEL").cast(pl.String)).drop_nulls().unique()
    mapping_lazy = unique_categories.with_columns(
        pl.col("PRODUCT_CATEGORY_LABEL")
        .map_elements(map_category, return_dtype=pl.String)
        .alias("BROAD_CATEGORY")
    ).lazy()
    
    # --- dim_date ---
    print("  -> Creating dim_date...")
    dim_date = (
        silver_lazy
        .select("DATE")
        .drop_nulls()
        .unique()
        .with_columns([
            pl.col("DATE").dt.strftime("%Y%m%d").cast(pl.Int32).alias("date_key"),
            pl.col("DATE").alias("full_date"),
            pl.col("DATE").dt.year().alias("year"),
            pl.col("DATE").dt.month().alias("month"),
            pl.col("DATE").dt.week().alias("week_of_year"),
            (pl.col("DATE") - pl.duration(days=364)).alias("same_week_last_year_date")
        ])
        .with_columns(
            pl.col("same_week_last_year_date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("same_week_last_year_key")
        )
        .drop("DATE")
    )
    dim_date.sink_parquet(settings.GOLD_DIM_DATE)
    
    # --- dim_product ---
    print("  -> Creating dim_product...")
    dim_product = (
        silver_lazy
        .select(["PLN", "PLN_LABEL", "OPSTUDY_LABEL", "PRODUCT_CATEGORY_LABEL", "BU"])
        .drop_nulls(subset=["PLN"])
        .unique(subset=["PLN"]) # Ensure exactly 1 row per PLN
        .with_columns(pl.col("PRODUCT_CATEGORY_LABEL").cast(pl.String))
        .join(mapping_lazy, on="PRODUCT_CATEGORY_LABEL", how="left")
        .select([
            pl.col("PLN").alias("pln_id"),
            pl.col("PLN_LABEL").alias("pln_label"),
            pl.col("OPSTUDY_LABEL").alias("opstudy_label"),
            pl.col("PRODUCT_CATEGORY_LABEL").alias("legacy_category_label"),
            pl.col("BROAD_CATEGORY").fill_null("Other / Miscellaneous").alias("broad_category"),
            pl.col("BU").alias("business_unit")
        ])
    )
    dim_product.sink_parquet(settings.GOLD_DIM_PRODUCT)
    
    # --- dim_segment ---
    print("  -> Creating dim_segment...")
    dim_segment = (
        silver_lazy
        .select("SEG")
        .drop_nulls()
        .unique()
        .select(pl.col("SEG").alias("segment_id"))
    )
    dim_segment.sink_parquet(settings.GOLD_DIM_SEGMENT)
    
    # --- fact_sales ---
    print("  -> Creating fact_sales...")
    fact_sales = (
        silver_lazy
        .select([
            pl.col("DATE").dt.strftime("%Y%m%d").cast(pl.Int32).alias("date_key"),
            pl.col("PLN").alias("pln_id"),
            pl.col("SEG").alias("segment_id"),
            (pl.col("PROMO") == "Y").alias("is_promo"),
            pl.col("ACTUAL").alias("actual_sales")
        ])
    )
    fact_sales.sink_parquet(settings.GOLD_FACT_SALES)
    
    print("Successfully generated Star Schema Parquet files.")

    print("\n--- Pipeline Complete ---")