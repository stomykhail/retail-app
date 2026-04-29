resource "aws_s3_bucket" "athena_results" {
  bucket        = "retail-legacy-dev-athena-results-mykhailo-2026"
  force_destroy = true
}

resource "aws_athena_workgroup" "retail_workgroup" {
  name = "retail_workgroup_dev"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/queries/"
    }
  }
}

resource "aws_glue_catalog_database" "retail_analytics" {
  name = "retail_analytics_dev"
}
