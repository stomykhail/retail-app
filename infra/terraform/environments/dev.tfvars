project_name = "retail-legacy-processing"
environment  = "dev"
aws_region   = "us-east-1"

# Bucket names must be globally unique in AWS.
raw_bucket_name         = "retail-legacy-dev-raw-mykhailo-2026"
transformed_bucket_name = "retail-legacy-dev-transformed-mykhailo-2026"

additional_tags = {
  Owner = "data-engineering"
}
