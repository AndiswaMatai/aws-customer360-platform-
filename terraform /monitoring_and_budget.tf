# Monitoring & cost control — CloudWatch alarms wired to SNS, plus an AWS
# Budget for spend alerting. Mirrors the alert tiers in the Azure flagship's
# monitoring/alert_rules.tf: pipeline failure (critical), DQ failure
# (high), runtime anomaly (cost/ops signal).

resource "aws_sns_topic" "platform_alerts" {
  name = "${local.prefix}-alerts"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.platform_alerts.arn
  protocol  = "email"
  endpoint  = var.owner_email
}

# Alarm 1: Step Functions execution failure — the medallion orchestration broke
resource "aws_cloudwatch_metric_alarm" "step_functions_failure" {
  alarm_name          = "${local.prefix}-sfn-execution-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  metric_name          = "ExecutionsFailed"
  namespace            = "AWS/States"
  period               = 300
  statistic            = "Sum"
  threshold            = 0
  alarm_description    = "Fires when the medallion_orchestration state machine fails — equivalent severity to the Azure platform's alert-adf-pipeline-failure rule."
  alarm_actions        = [aws_sns_topic.platform_alerts.arn]

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.medallion_orchestration.arn
  }
}

# Alarm 2: Glue job failure — distinguishes "a specific ETL stage broke" from
# overall orchestration failure, same logic as the Azure DQ-gate-specific alert.
resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "${local.prefix}-glue-job-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  metric_name          = "glue.driver.aggregate.numFailedTasks"
  namespace            = "Glue"
  period               = 300
  statistic            = "Sum"
  threshold            = 0
  alarm_description    = "Fires when either Glue ETL job (staged_transform or curated_transform) has failed tasks."
  alarm_actions        = [aws_sns_topic.platform_alerts.arn]
  treat_missing_data    = "notBreaching"
}

# Alarm 3: Kinesis iterator age — clickstream consumer falling behind (real-time SLA breach)
resource "aws_cloudwatch_metric_alarm" "kinesis_consumer_lag" {
  alarm_name          = "${local.prefix}-kinesis-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 2
  metric_name          = "GetRecords.IteratorAgeMilliseconds"
  namespace            = "AWS/Kinesis"
  period               = 300
  statistic            = "Maximum"
  threshold            = 60000 # 60 seconds — beyond this, real-time clickstream is no longer "real-time"
  alarm_description    = "Fires when the Lambda consumer falls more than 60s behind the Kinesis stream — investigate Lambda concurrency/throttling first."
  alarm_actions        = [aws_sns_topic.platform_alerts.arn]

  dimensions = {
    StreamName = aws_kinesis_stream.clickstream.name
  }
}

# Alarm 4: Redshift Serverless RPU usage spike — cost guardrail
resource "aws_cloudwatch_metric_alarm" "redshift_rpu_spike" {
  alarm_name          = "${local.prefix}-redshift-rpu-spike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  metric_name          = "ComputeCapacity"
  namespace            = "AWS/Redshift-Serverless"
  period               = 900
  statistic            = "Maximum"
  threshold            = 64 # alert if usage scales well beyond the configured base_capacity
  alarm_description    = "Redshift Serverless auto-scaled to 64+ RPUs — investigate a runaway query before it drives an unexpected bill."
  alarm_actions         = [aws_sns_topic.platform_alerts.arn]
  treat_missing_data    = "notBreaching"

  dimensions = {
    Namespace = aws_redshiftserverless_namespace.this.namespace_name
  }
}

# AWS Budget — monthly spend ceiling with 80%/100% notifications
resource "aws_budgets_budget" "monthly" {
  name         = "${local.prefix}-monthly-budget"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type              = "PERCENTAGE"
    notification_type           = "ACTUAL"
    subscriber_email_addresses = [var.owner_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type              = "PERCENTAGE"
    notification_type           = "FORECASTED"
    subscriber_email_addresses = [var.owner_email]
  }
}

# ── Outputs ──────────────────────────────────────────────────────────────────
output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "curated_bucket_name" {
  value = aws_s3_bucket.curated.bucket
}

output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.medallion_orchestration.arn
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.clickstream.name
}
