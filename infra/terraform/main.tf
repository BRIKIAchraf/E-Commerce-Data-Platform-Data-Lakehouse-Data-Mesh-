# Terraform configuration for E-commerce Data Platform
terraform {
  required_version = ">= 1.4.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.60"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- S3 Buckets for Data Lake Medallion Architecture ---
resource "aws_s3_bucket" "raw_bucket" {
  bucket        = "${var.project_name}-raw-lake"
  force_destroy = true
}

resource "aws_s3_bucket" "curated_bucket" {
  bucket        = "${var.project_name}-curated-lake"
  force_destroy = true
}

resource "aws_s3_bucket" "ml_bucket" {
  bucket        = "${var.project_name}-ml-lake"
  force_destroy = true
}

# --- AWS Managed Streaming for Apache Kafka (MSK) ---
resource "aws_msk_cluster" "kafka_cluster" {
  cluster_name           = "${var.project_name}-msk"
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.subnet_ids
    security_groups = [var.security_group_id]
  }
}

# --- Snowflake Database and Schema creation ---
provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_user
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

resource "snowflake_database" "db" {
  name    = "ECOMMERCE_DB"
  comment = "Database containing staging, intermediate, and marts models for BI and ML"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.db.name
  name     = "STAGING"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.db.name
  name     = "MARTS"
}
