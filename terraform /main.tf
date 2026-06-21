# AWS Customer 360 Platform — Infrastructure
#
# Provisions: S3 data lake (raw/staged/curated), Glue Catalog + ETL jobs,
# Kinesis Data Stream for real-time clickstream, Lambda trigger, Step
# Functions orchestration, Redshift Serverless for the analytics layer,
# IAM roles, CloudWatch monitoring, and AWS Budgets for cost control.
#
# Usage:
#   terraform init
#   terraform plan -var-file="environments/dev.tfvars"
#   terraform apply -var-file="environments/dev.tfvars"

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }

  backend "s3" {
    # Configure via -backend-config flags per environment.
    # Never hardcode backend state credentials here.
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

locals {
  project     = "customer360"
  environment = var.environment
  prefix      = "${local.project}-${local.environment}"

  common_tags = {
    Project     = "aws-customer-360-platform"
    Environment = var.environment
    ManagedBy   = "terraform"
    CostCenter  = var.cost_center
    Owner       = var.owner_email
  }
}

data "aws_caller_identity" "current" {}
