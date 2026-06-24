# AWS Customer 360 Platform

![Sector](https://img.shields.io/badge/Sector-Flagship%20%C2%B7%20AWS-FF9900?style=flat&logo=amazonaws)
![CI](https://img.shields.io/badge/CI-passing-0f7a4b?style=flat&logo=githubactions)
![IaC](https://img.shields.io/badge/IaC-Terraform-7B42BC?style=flat&logo=terraform)
![Python](https://img.shields.io/badge/Python-3.12-blue?style=flat&logo=python)

**[← Back to live portfolio](https://andiswamatai.github.io)**

# ☁️ AWS Customer 360 Platform

![Sector](https://img.shields.io/badge/Sector-Ecommerce%20%2F%20Customer%20360-1F3864?style=flat)
![Cloud](https://img.shields.io/badge/Cloud-AWS-orange?style=flat)
![Architecture](https://img.shields.io/badge/Architecture-Lakehouse%20%7C%20Streaming%20%7C%20Batch-blue?style=flat)
![CI/CD](https://img.shields.io/badge/DevOps-Terraform%20%7C%20GitHub%20Actions-purple?style=flat)

---

## 🚀 Overview

A full end-to-end AWS Customer 360 data platform that unifies real-time clickstream ingestion and batch transaction processing into a single governed analytics model.

The platform combines **Kinesis + Lambda (streaming)** with **AWS Glue (batch ETL)** and orchestrates workloads via **Step Functions**, delivering curated Customer 360 datasets in **Redshift Serverless** for analytics and reporting.

This project demonstrates production-grade AWS data engineering architecture aligned with real-world e-commerce and marketing analytics systems.

---

## 🧠 Why this exists

Customer 360 is one of the most complex problems in modern data engineering:

- High-volume, anonymous clickstream data arrives in real time
- Structured transaction data arrives in batch form
- Identity resolution must unify both into a single customer view
- Latency must remain low enough for marketing and CRM activation

This platform solves that by designing **dual ingestion paths (stream + batch) converging into a single governed analytics layer**.
---
## Solutions Overview

This platform implements a cloud-native Customer 360 architecture on AWS.

The system:

- Ingests streaming clickstream events via Kinesis + Lambda
- Processes batch transactional data via AWS Glue ETL jobs
- Orchestrates workflows using Step Functions
- Applies data quality validation before downstream processing
- Builds a unified Customer 360 dataset in Redshift Serverless

## Architecture

``📡 Data Sources
- Clickstream events (web/app activity)
- E-commerce transactions
- Customer interaction logs

        ↓

⚡ Streaming Layer
- Amazon Kinesis
- AWS Lambda (event batching + preprocessing)

        ↓

📦 Batch Processing Layer
- AWS Glue ETL Jobs
- Scheduled ingestion of structured data

        ↓

🎼 Orchestration Layer
- AWS Step Functions
- Pipeline coordination + data quality gate

        ↓

🥇 Curated Layer
- S3 (Parquet datasets)
- Cleaned + conformed Customer 360 model

        ↓

📊 Analytics Layer
- Amazon Redshift Serverless
- Spectrum external tables for zero-copy analytics
```

## What's actually runnable vs. what's reference architecture

| Component | Status |
|---|---|
| `engine/` — full raw→staged→curated pipeline | **Runs locally**, pandas, no AWS account needed |
| `data_quality/` — completeness, uniqueness, referential integrity, freshness | **Runs locally**, tested |
| `cost_optimization/cost_calculator.py` | **Runs locally**, models real savings from the Terraform config |
| `tests/` | **Runs locally**, 7 passing unit tests |
| `terraform/*.tf` | **Valid HCL**, `terraform validate`-able, not applied (no AWS account) |
| `glue_jobs/*.py` | **Valid PySpark**, written exactly as it would run as a Glue job, mirrors `engine/` 1:1 |
| `lambda/clickstream_processor.py` | **Valid, deployable Lambda handler**, no AWS-specific mocking needed beyond `boto3` |
| `step_functions/*.json` | **Valid Amazon States Language**, matches the real Step Functions schema |
| `monitoring/*.json` | **Valid CloudWatch dashboard schema** |
| `.github/workflows/cd.yml` | **Documents the real deployment commands**, doesn't execute against live infra |

## Repository Structure

```
engine/                  Local-runnable pipeline (raw → staged → curated)
glue_jobs/                Production PySpark ETL scripts (1:1 mirror of engine/)
lambda/                   Kinesis clickstream consumer
step_functions/           State machine orchestrating Glue jobs + DQ gate
terraform/                Full IaC: S3, Glue, Kinesis, Lambda, Step Functions, Redshift
monitoring/                CloudWatch alarms (Terraform) + dashboard JSON
cost_optimization/        Working cost model + the controls it measures
data_quality/             Standalone DQ framework (completeness/unique/RI/freshness)
tests/                    Unit tests for engine + DQ framework
.github/workflows/        CI, Terraform Plan, CD
```

## Why Both Real & Streaming 

This architecture intentionally combines both ingestion patterns:

### 🟦 Batch (Glue)
- Daily structured transaction ingestion
- Cost-efficient ETL processing
- Ideal for stable, known datasets

### ⚡ Streaming (Kinesis + Lambda)
- High-volume clickstream ingestion (~700K+ events)
- Near real-time behavioural tracking
- Micro-batching to reduce Lambda invocation cost

Both pipelines converge into a unified Customer 360 model in S3 and Redshift.

## Sample Output

```
CONVERSION FUNNEL (all customers)
         stage  event_count
     page_view       173176
  product_view        96296
   add_to_cart        49998
checkout_start        27110
      purchase        19142

Average customer conversion rate: 22.65%

DATA QUALITY: 0 of 7 checks failed. Curated layer is safe to load into Redshift.

COST OPTIMIZATION:
  S3 lifecycle tiering savings:    $21.82/month  (48.5%)
  Glue job bookmarking savings:    $133.98/month (84.6%)
  Redshift Serverless savings:     $660.00/month (55.0%) vs always-on equivalent
  Lambda batching savings:         $5.47/month   (99.3%)
  TOTAL ESTIMATED ANNUAL SAVINGS:  $9,855.22
```

## 🧠 Engineering Design Principles

This platform demonstrates:

- Lambda-based event-driven ingestion
- Batch + streaming hybrid architecture
- Idempotent ETL via Glue bookmarking
- Event-driven orchestration using Step Functions
- Data lake architecture using S3 (Parquet format)
- Zero-copy analytics using Redshift Spectrum
- Infrastructure-as-Code (Terraform)
- CI/CD-driven deployment lifecycle
- Cost-aware cloud engineering design

## Business Value

This system enables organisations to:

- Build a unified Customer 360 view across all channels
- Enable real-time marketing and behavioural analytics
- Improve conversion tracking accuracy
- Reduce infrastructure costs through optimisation
- Support scalable analytics on millions of events

## Production Enhancement

If deployed in enterprise AWS environments:

- Kinesis Data Streams for real-time ingestion scaling
- AWS Glue Crawlers for automated schema discovery
- Step Functions for resilient workflow orchestration
- S3 data lake with lifecycle policies (Bronze/Silver/Gold)
- Redshift Serverless for scalable analytics queries
- CloudWatch for observability and alerting
- IAM least-privilege role-based security model
- Terraform for full infrastructure lifecycle management
