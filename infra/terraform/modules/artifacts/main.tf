# One encrypted bucket for ML artifacts, split by prefix:
#   mlflow/            model artifacts (written by the MLflow server)
#   holdouts/          frozen evaluation sets (read by CI)
#   prediction-logs/   served predictions (written by Fluent Bit, read by the drift job)
#   labels/            real outcomes joined to predictions for concept drift

variable "bucket_name" {
  type = string
}

variable "prediction_log_retention_days" {
  type    = number
  default = 180
}

variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_kms_key" "artifacts" {
  description         = "${var.bucket_name} encryption"
  enable_key_rotation = true
  tags                = var.tags
}

resource "aws_kms_alias" "artifacts" {
  name          = "alias/${var.bucket_name}"
  target_key_id = aws_kms_key.artifacts.key_id
}

resource "aws_s3_bucket" "this" {
  bucket = var.bucket_name
  tags   = var.tags
}

# Versioning means an overwritten or deleted model artifact or holdout can be recovered
resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.artifacts.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    id     = "expire-prediction-logs"
    status = "Enabled"

    filter {
      prefix = "prediction-logs/"
    }

    expiration {
      days = var.prediction_log_retention_days
    }
  }
}

# Refuse any request that isn't over TLS
data "aws_iam_policy_document" "tls_only" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.this.arn,
      "${aws_s3_bucket.this.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.tls_only.json

  depends_on = [aws_s3_bucket_public_access_block.this]
}

output "bucket_name" {
  value = aws_s3_bucket.this.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.this.arn
}

output "kms_key_arn" {
  value = aws_kms_key.artifacts.arn
}
