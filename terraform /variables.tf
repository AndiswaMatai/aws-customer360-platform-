variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "af-south-1" # Cape Town region
}

variable "cost_center" {
  type    = string
  default = "data-engineering"
}

variable "owner_email" {
  type    = string
  default = "andiswacebekhulu1@gmail.com"
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base RPU capacity (scales automatically above this)"
  type        = number
  default     = 8 # minimum supported; cost optimization lever, see cost_optimization/
}

variable "kinesis_shard_count" {
  description = "Kinesis Data Stream shard count for the clickstream ingest"
  type        = number
  default     = 2
}

variable "glue_worker_type" {
  description = "Glue ETL job worker type"
  type        = string
  default     = "G.1X" # cost-optimized default; G.2X for memory-heavy jobs
}

variable "glue_number_of_workers" {
  type    = number
  default = 4
}

variable "monthly_budget_usd" {
  description = "Monthly AWS spend budget in USD before cost alerts fire"
  type        = number
  default     = 600
}

variable "log_retention_days" {
  type    = number
  default = 30
}
