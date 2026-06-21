# AWS Glue — Data Catalog (schema registry) + ETL jobs that run the
# staged/curated transforms in glue_jobs/, mirroring engine/medallion_pipeline.py.

resource "aws_glue_catalog_database" "raw" {
  name = "${local.project}_raw"
}

resource "aws_glue_catalog_database" "staged" {
  name = "${local.project}_staged"
}

resource "aws_glue_catalog_database" "curated" {
  name = "${local.project}_curated"
}

# Crawler: scans the raw S3 bucket and infers/updates the schema in the
# Glue Catalog automatically — handles upstream schema drift without a
# manual DDL change.
resource "aws_glue_crawler" "raw" {
  name          = "${local.prefix}-raw-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.raw.name

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/"
  }

  schedule = "cron(0 1 * * ? *)" # 1 AM daily, ahead of the ETL job run

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_iam_role" "glue_role" {
  name = "${local.prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
      Resource = [
        aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*",
        aws_s3_bucket.staged.arn, "${aws_s3_bucket.staged.arn}/*",
        aws_s3_bucket.curated.arn, "${aws_s3_bucket.curated.arn}/*",
      ]
    }]
  })
}

# ETL Job 1: raw -> staged (cleansing, dedup, validation)
# Glue equivalent of engine/medallion_pipeline.py::staged_clean_*()
resource "aws_glue_job" "staged_transform" {
  name              = "${local.prefix}-staged-transform"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30 # minutes
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.bucket}/scripts/staged_transform.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"   = "job-bookmark-enable" # cost optimization: only processes new files, never reprocesses
    "--enable-metrics"        = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"               = "s3://${aws_s3_bucket.staged.bucket}/_glue_temp/"
  }
}

# ETL Job 2: staged -> curated (Customer 360 join + aggregates)
# Glue equivalent of engine/medallion_pipeline.py::curated_customer_360()
resource "aws_glue_job" "curated_transform" {
  name              = "${local.prefix}-curated-transform"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.bucket}/scripts/curated_transform.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable"
    "--enable-metrics"      = "true"
    "--TempDir"             = "s3://${aws_s3_bucket.curated.bucket}/_glue_temp/"
  }
}
