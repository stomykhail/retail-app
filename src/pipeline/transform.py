import polars as pl
import polars.selectors as cs
from src.config.settings import OPSTUDY_POLISH

def clean_raw_data(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """Applies data cleaning plan to the raw DataFrame."""
    # 2. Rename PROMO column if it has extra characters like 'PROMO,,'
    promo_col = [col for col in lazy_df.collect_schema().names() if "PROMO" in col]
    if promo_col and promo_col[0] != "PROMO":
        lazy_df = lazy_df.rename({promo_col[0]: "PROMO"})

    # 3. Text Cleansing & Date Parsing
    clean_df = lazy_df.with_columns([
        # Strip literal quotes BEFORE casting to integer to prevent crashes
        (pl.col("WEEK").cast(pl.String).str.replace_all('"', '').cast(pl.Int64) + 19000000)
        .cast(pl.String)
        .str.to_date("%Y%m%d")
        .alias("DATE"),
        
        # Strip literal quotes and trailing commas
        pl.col("PROMO").cast(pl.String).str.replace_all('"', '').str.replace_all(",,", ""),

        # Fix non-typical SEG values (e.g. NoSegment-C -> C)
        pl.col("SEG").cast(pl.String).str.replace_all("NoSegment-", ""),

        # Strip quotes/spaces and convert labels to UPPERCASE immediately
        pl.col("PRODUCT_CATEGORY_LABEL").cast(pl.String).str.replace_all('"', '').str.strip_chars().str.to_uppercase(),
        pl.col("OPSTUDY_LABEL").cast(pl.String).str.replace_all('"', '').str.strip_chars().str.to_uppercase(),
        pl.col("PLN_LABEL").cast(pl.String).str.replace_all('"', '').str.strip_chars().str.to_uppercase()
    ])

    # 4. Validation Logic (Quarantine Layer tagging)
    clean_df = clean_df.with_columns(
        pl.when(pl.col("PLN").cast(pl.String) == "PLN").then(pl.lit("Header Pollution"))
        .when(pl.col("PLN").is_null() | pl.col("ACTUAL").is_null()).then(pl.lit("Missing Critical Keys"))
        .otherwise(pl.lit("Valid"))
        .alias("ERROR_REASON")
    )
    
    return clean_df

def process_valid_data(clean_df: pl.DataFrame) -> pl.DataFrame:
    """Takes the in-memory cleaned dataframe, filters valid rows, and applies golden record deduplication."""
    
    # Drop quarantine rows to free up memory
    valid_df = clean_df.filter(pl.col("ERROR_REASON") == "Valid").drop("ERROR_REASON")

    # 5. Deduplicate SECOND (Faster because dataset is smaller now)
    valid_df = valid_df.unique()

    # 6. Standardize Missing/Null Values BEFORE building the Golden Record
    null_aliases = ["na", "none", "unknown", "nan", "null", "", "nobusinessunit"]
    valid_df = valid_df.with_columns(
        pl.when(cs.string().str.to_lowercase().str.strip_chars().is_in(null_aliases))
        .then(pl.lit(None))
        .otherwise(cs.string())
        .name.keep()
    )

    # 7. Build the "Golden Record" Table (Optimized: Sub-sorting instead of Global sorting)
    golden_record = (
        valid_df
        .group_by("PLN")
        .agg([
            pl.col("PLN_LABEL").sort_by("DATE", descending=True).drop_nulls().first(),
            pl.col("BU").sort_by("DATE", descending=True).drop_nulls().first(),
            pl.col("PRODUCT_CATEGORY_LABEL").sort_by("DATE", descending=True).drop_nulls().first(),
            pl.col("OPSTUDY_LABEL").sort_by("DATE", descending=True).drop_nulls().first()
        ])
    )

    # 8. The Metadata Heal (Join)
    sales_data = valid_df.drop(["PLN_LABEL", "BU", "PRODUCT_CATEGORY_LABEL", "OPSTUDY_LABEL"])
    healed_df = sales_data.join(golden_record, on="PLN", how="left")

    # Light Polish for OPSTUDY descriptions
    healed_df = healed_df.with_columns(
        pl.col("OPSTUDY_LABEL").replace_strict(OPSTUDY_POLISH, default=pl.col("OPSTUDY_LABEL"))
    )

    # 9. Impute and Optimize Types
    healed_df = healed_df.with_columns([
        pl.col("PROMO").fill_null("N").cast(pl.Categorical),
        pl.col(["BU", "SEG", "PRODUCT_CATEGORY_LABEL", "OPSTUDY_LABEL", "PLN_LABEL"])
        .fill_null("Unknown")
        .cast(pl.Categorical),
        pl.col("ACTUAL").cast(pl.Float32)
    ]).drop("WEEK") # Drop the old string WEEK column to save memory

    return healed_df