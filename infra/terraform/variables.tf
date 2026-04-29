variable "project_name" {
  description = "Project identifier for naming and tagging."
  type        = string
  default     = "retail-legacy-processing"
}

variable "environment" {
  description = "Deployment environment (for example: dev, stage, prod)."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region where infrastructure will be provisioned."
  type        = string
  default     = "us-east-1"
}

variable "raw_bucket_name" {
  description = "S3 bucket name for raw CSV data."
  type        = string
}

variable "transformed_bucket_name" {
  description = "S3 bucket name for transformed parquet data."
  type        = string
}

variable "raw_bucket_retention_days" {
  description = "Lifecycle expiration (days) for objects in raw bucket."
  type        = number
  default     = 365
}

variable "transformed_bucket_retention_days" {
  description = "Lifecycle expiration (days) for objects in transformed bucket."
  type        = number
  default     = 730
}

variable "additional_tags" {
  description = "Additional tags applied to all resources."
  type        = map(string)
  default     = {}
}
