CREATE EXTERNAL TABLE IF NOT EXISTS retail_analytics_dev.fact_sales (
    date_key INT,
    pln_id BIGINT,
    segment_id STRING,
    is_promo BOOLEAN,
    actual_sales FLOAT
)
STORED AS PARQUET
LOCATION 's3://retail-legacy-dev-transformed-mykhailo-2026/gold/fact_sales/';

CREATE EXTERNAL TABLE IF NOT EXISTS retail_analytics_dev.dim_date (
    full_date DATE,
    date_key INT,
    year INT,
    month INT,
    week_of_year INT,
    same_week_last_year_key INT
)
STORED AS PARQUET
LOCATION 's3://retail-legacy-dev-transformed-mykhailo-2026/gold/dim_date/';

CREATE EXTERNAL TABLE IF NOT EXISTS retail_analytics_dev.dim_product (
    pln_id BIGINT,
    pln_label STRING,
    legacy_category_label STRING,
    opstudy_label STRING,
    business_unit STRING,
    broad_category STRING
)
STORED AS PARQUET
LOCATION 's3://retail-legacy-dev-transformed-mykhailo-2026/gold/dim_product/';

CREATE EXTERNAL TABLE IF NOT EXISTS retail_analytics_dev.dim_segment (
    segment_id STRING
)
STORED AS PARQUET
LOCATION 's3://retail-legacy-dev-transformed-mykhailo-2026/gold/dim_segment/';
