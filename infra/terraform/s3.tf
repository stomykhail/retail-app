locals {
  s3_common_tags = {
    Layer = "storage"
  }
}

resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
  tags   = merge(local.s3_common_tags, { DataZone = "raw" })
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "expire-raw-objects"
    status = "Enabled"
    filter {}

    expiration {
      days = var.raw_bucket_retention_days
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "transformed" {
  bucket = var.transformed_bucket_name
  tags   = merge(local.s3_common_tags, { DataZone = "transformed" })
}

resource "aws_s3_bucket_versioning" "transformed" {
  bucket = aws_s3_bucket.transformed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "transformed" {
  bucket = aws_s3_bucket.transformed.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "transformed" {
  bucket = aws_s3_bucket.transformed.id

  rule {
    id     = "expire-transformed-objects"
    status = "Enabled"
    filter {}

    expiration {
      days = var.transformed_bucket_retention_days
    }
  }
}

resource "aws_s3_bucket_public_access_block" "transformed" {
  bucket                  = aws_s3_bucket.transformed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}