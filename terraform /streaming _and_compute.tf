# Real-time clickstream ingestion: Kinesis -> Lambda -> S3 raw zone,
# running alongside the batch Glue pipeline for transactions/customers/products.

resource "aws_kinesis_stream" "clickstream" {
  name             = "${local.prefix}-clickstream"
  shard_count      = var.kinesis_shard_count
  retention_period = 24 # hours

  stream_mode_details {
    stream_mode = "PROVISIONED" # cost optimization: ON_DEMAND for spiky/unpredictable traffic instead
  }

  tags = local.common_tags
}

resource "aws_iam_role" "lambda_role" {
  name = "${local.prefix}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_kinesis_s3" {
  name = "lambda-kinesis-s3"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListStreams"]
        Resource = aws_kinesis_stream.clickstream.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.raw.arn}/clickstream_events/*"
      }
    ]
  })
}

# Lambda function: batches Kinesis records and writes them to the raw S3
# zone as Parquet — see lambda/clickstream_processor.py
resource "aws_lambda_function" "clickstream_processor" {
  function_name = "${local.prefix}-clickstream-processor"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.12"
  handler       = "clickstream_processor.handler"
  filename      = "../lambda/clickstream_processor.zip" # built by .github/workflows/cd.yml
  timeout       = 60
  memory_size   = 256 # cost optimization: right-sized for a batching/passthrough workload, not compute-heavy

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.raw.bucket
    }
  }

  tags = local.common_tags
}

resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn  = aws_kinesis_stream.clickstream.arn
  function_name     = aws_lambda_function.clickstream_processor.arn
  starting_position = "LATEST"
  batch_size        = 500
  maximum_batching_window_in_seconds = 30 # cost optimization: batches before invoking, fewer Lambda invocations
}

# Step Functions: orchestrates the batch Glue jobs (crawler -> staged -> curated -> DQ gate)
resource "aws_sfn_state_machine" "medallion_orchestration" {
  name     = "${local.prefix}-medallion-orchestration"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = file("${path.module}/../step_functions/medallion_state_machine.json")

  tags = local.common_tags
}

resource "aws_iam_role" "step_functions_role" {
  name = "${local.prefix}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "step_functions_glue_access" {
  name = "sfn-glue-access"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:StartCrawler", "glue:GetCrawler"]
      Resource = "*"
    }]
  })
}

# EventBridge: triggers Step Functions daily, same cadence as the batch ADF
# trigger in the Azure flagship platform.
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "${local.prefix}-daily-trigger"
  schedule_expression = "cron(0 2 * * ? *)" # 2 AM UTC daily
  state               = var.environment == "prod" ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "step_functions" {
  rule     = aws_cloudwatch_event_rule.daily_trigger.name
  arn      = aws_sfn_state_machine.medallion_orchestration.arn
  role_arn = aws_iam_role.eventbridge_sfn_role.arn
}

resource "aws_iam_role" "eventbridge_sfn_role" {
  name = "${local.prefix}-eventbridge-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn_policy" {
  name = "eventbridge-start-sfn"
  role = aws_iam_role.eventbridge_sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.medallion_orchestration.arn
    }]
  })
}

# Redshift Serverless — the analytics layer queried by QuickSight/Power BI,
# loaded from the curated S3 zone via Redshift Spectrum external tables
# (no ETL duplication: Spectrum queries the curated Parquet files directly).
resource "aws_redshiftserverless_namespace" "this" {
  namespace_name = "${local.prefix}-ns"
  db_name        = "customer360"
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name     = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name      = "${local.prefix}-wg"
  base_capacity       = var.redshift_base_capacity # cost optimization: scales up automatically, scales back down, billed per-second
  publicly_accessible = false
}
