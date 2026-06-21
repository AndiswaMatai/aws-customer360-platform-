# S3 Data Lake — raw / staged / curated buckets, matching the local
# engine's data/lake/{raw,staged,curated} structure exactly. Lifecycle
# rules implement the cost optimization documented in cost_optimization/.

resource "aws_s3_bucket" "raw" {
  bucket = "${local.prefix}-raw-${data.aws_caller_identity.current.account_id}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "staged" {
  bucket = "${local.prefix}-staged-${data.aws_caller_identity.current.account_id}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "curated" {
  bucket = "${local.prefix}-curated-${data.aws_caller_identity.current.account_id}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Cost optimization: raw lands in Standard, transitions to Infrequent Access
# after 30 days, Glacier after 90 — raw data is rarely re-read once staged.
resource "aws_s3_bucket_lifecycle_configuration" "raw_tiering" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "raw-tiering"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 1825 # 5 years, typical retail/e-commerce retention requirement
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "staged_tiering" {
  bucket = aws_s3_bucket.staged.id

  rule {
    id     = "staged-tiering"
    status = "Enabled"

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
  }
}

# Curated stays in Standard — this is the layer Redshift/QuickSight query directly,
# IA/Glacier retrieval latency would hurt dashboard performance.

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "staged" {
  bucket                  = aws_s3_bucket.staged.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
