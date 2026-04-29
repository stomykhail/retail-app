data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_pipeline_role" {
  name               = "glue_pipeline_role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_pipeline_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3_limited_access" {
  statement {
    sid    = "RawBucketList"
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.raw.arn
    ]
  }

  statement {
    sid    = "RawBucketObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.raw.arn}/*"
    ]
  }

  statement {
    sid    = "TransformedBucketList"
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.transformed.arn
    ]
  }

  statement {
    sid    = "TransformedBucketObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.transformed.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "glue_s3_limited_access" {
  name        = "${var.project_name}-${var.environment}-glue-s3-limited-access"
  description = "Allow Glue to access only raw and transformed S3 buckets."
  policy      = data.aws_iam_policy_document.glue_s3_limited_access.json
}

resource "aws_iam_role_policy_attachment" "glue_s3_limited_access" {
  role       = aws_iam_role.glue_pipeline_role.name
  policy_arn = aws_iam_policy.glue_s3_limited_access.arn
}
