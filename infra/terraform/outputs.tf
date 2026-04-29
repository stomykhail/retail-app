output "raw_bucket_name" {
  description = "S3 bucket for raw CSV files."
  value       = aws_s3_bucket.raw.bucket
}

output "transformed_bucket_name" {
  description = "S3 bucket for transformed parquet files."
  value       = aws_s3_bucket.transformed.bucket
}
