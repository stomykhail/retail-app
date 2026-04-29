resource "aws_glue_job" "transform_raw_to_parquet" {
  name     = "${var.project_name}-${var.environment}-glue-transform"
  role_arn = aws_iam_role.glue_pipeline_role.arn

  glue_version = "3.0"
  max_capacity = 1.0

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.raw.bucket}/scripts/glue_transform.py"
  }

  default_arguments = {
    "--additional-python-modules" = "polars==0.20.10,s3fs==2023.12.2,pyarrow"
    "--extra-py-files"            = "s3://${aws_s3_bucket.raw.bucket}/scripts/retail_sales_project-0.1.0-py3-none-any.whl"
    "--raw-bucket"                = aws_s3_bucket.raw.bucket
    "--transformed-bucket"        = aws_s3_bucket.transformed.bucket
  }

  execution_property {
    max_concurrent_runs = 1
  }
}
