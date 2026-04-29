import argparse
import logging
import os
import sys
import tempfile
import gc

# Add the project root to the Python path to resolve 'src' module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import boto3
import polars as pl

from src.config import settings
from src.pipeline import transform

logger = logging.getLogger(__name__)


def build_star_schema(df: pl.DataFrame):
    """Transforms the flat cleaned dataframe into a Star Schema."""
    logger.info("Building Star Schema Dimensions and Fact tables...")

    # 1. Dim Date
    dim_date = df.select(pl.col("DATE").alias("full_date")).unique().drop_nulls()
    dim_date = dim_date.with_columns([
        pl.col("full_date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("date_key"),
        pl.col("full_date").dt.year().alias("year"),
        pl.col("full_date").dt.month().alias("month"),
        pl.col("full_date").dt.strftime("%V").cast(pl.Int32).alias("week_of_year"),
        (pl.col("full_date").dt.strftime("%Y%m%d").cast(pl.Int32) - 10000).alias("same_week_last_year_key")
    ])

    # 2. Dim Segment
    dim_segment = df.select(pl.col("SEG").alias("segment_id")).unique().drop_nulls()

    # 3. Dim Product
    dim_product = df.select([
        pl.col("PLN").alias("pln_id"),
        pl.col("PLN_LABEL").alias("pln_label"),
        pl.col("PRODUCT_CATEGORY_LABEL").alias("legacy_category_label"),
        pl.col("OPSTUDY_LABEL").alias("opstudy_label"),
        pl.col("BU").alias("business_unit")
    ]).unique()

    # Dynamically apply Category Mapping from settings if it exists, else default to legacy
    if hasattr(settings, 'EXACT_MATCHES'):
        exact_matches = getattr(settings, 'EXACT_MATCHES', {})
        dim_product = dim_product.with_columns(
            pl.col("legacy_category_label").replace(exact_matches, default="Other").alias("broad_category")
        )
    else:
        dim_product = dim_product.with_columns(
            pl.col("legacy_category_label").alias("broad_category")
        )

    # 4. Fact Sales
    # Join date_key back to the main dataframe
    fact_sales = df.join(
        dim_date.select(["full_date", "date_key"]),
        left_on="DATE",
        right_on="full_date",
        how="inner"
    )

    fact_sales = fact_sales.select([
        pl.col("date_key"),
        pl.col("PLN").alias("pln_id"),
        pl.col("SEG").alias("segment_id"),
        (pl.col("PROMO") == "Y").alias("is_promo"),
        pl.col("ACTUAL").cast(pl.Float32).alias("actual_sales")
    ])

    logger.info(f"Schema built. Fact Sales rows: {fact_sales.height:,}")
    return dim_date, dim_product, dim_segment, fact_sales


def main() -> None:
    parser = argparse.ArgumentParser(description="AWS Glue job to transform cleaned CSV into Parquet Star Schema.")
    parser.add_argument(
        "--raw-bucket",
        default=os.environ.get("RAW_BUCKET"),
        help="S3 bucket name containing the raw/cleaned CSV.",
    )
    parser.add_argument(
        "--raw-key",
        default="raw/source_1150208_to_1171119_part.csv",
        help="S3 object key for the source CSV.",
    )
    parser.add_argument(
        "--transformed-bucket",
        default=os.environ.get("TRANSFORMED_BUCKET"),
        help="Destination S3 bucket name for Parquet files.",
    )
    
    # In AWS Glue, system arguments are sometimes passed with an extra '--' prefix due to how AWS injects them.
    # Using parse_known_args allows the script to safely ignore any AWS-specific internal arguments.
    args, _ = parser.parse_known_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    if not args.raw_bucket or not args.transformed_bucket:
        raise SystemExit("Missing S3 buckets. Pass arguments or set RAW_BUCKET / TRANSFORMED_BUCKET env vars.")

    s3_input_path = f"s3://{args.raw_bucket}/{args.raw_key}"

    logger.info(f"Downloading {s3_input_path} via boto3 (optimized multipart)...")
    s3 = boto3.client("s3")
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name
        
    try:
        s3.download_file(args.raw_bucket, args.raw_key, tmp_path)
        logger.info("Download complete. Scanning raw CSV with Polars...")
        lazy_df = pl.scan_csv(tmp_path, separator="|", ignore_errors=True, quote_char=None)
        
        logger.info("Applying initial cleaning rules...")
        cleaned_lazy = transform.clean_raw_data(lazy_df)
        df = cleaned_lazy.collect()
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    logger.info("Executing Golden Record Deduplication (process_valid_data)...")
    processed_df = transform.process_valid_data(df)

    # FREE MEMORY: Delete the raw dataframe now that deduplication is done
    del df
    gc.collect()

    dim_date, dim_product, dim_segment, fact_sales = build_star_schema(processed_df)

    # FREE MEMORY: Delete the combined processed dataframe now that the Star Schema is built
    del processed_df
    gc.collect()

    logger.info(f"Writing Parquet files to S3 bucket: {args.transformed_bucket}...")
    
    def save_and_upload(df: pl.DataFrame, filename: str):
        table_name = filename.split('.')[0]
        key = f"gold/{table_name}/{filename}"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            tmp_path = tmp.name
        try:
            df.write_parquet(tmp_path)
            s3.upload_file(tmp_path, args.transformed_bucket, key)
            logger.info(f"  Uploaded {key}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    save_and_upload(dim_date, "dim_date.parquet")
    save_and_upload(dim_product, "dim_product.parquet")
    save_and_upload(dim_segment, "dim_segment.parquet")
    save_and_upload(fact_sales, "fact_sales.parquet")
    
    logger.info("Glue transformation completed successfully!")


if __name__ == "__main__":
    main()