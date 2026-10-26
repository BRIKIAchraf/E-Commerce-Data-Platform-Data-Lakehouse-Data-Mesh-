

## About

Modern e-commerce data platform blueprint combining real-time streaming, batch lakehouse processing, ELT analytics, governance, data quality and ML serving.
# E-Commerce Data Platform

Modern data platform reference architecture combining real-time streaming, batch lakehouse processing, ELT analytics, governance and ML serving.

## Business objective

Provide reliable product, order and customer datasets for operational monitoring, analytics and personalised recommendations.

## Architecture

~~~text
Sources / CDC
    ├─→ Kafka → Flink → S3 / Snowflake
    └─→ Airbyte → S3
              ↓
        Delta Lake / PySpark
              ↓
     dbt Bronze → Silver → Gold
              ↓
        BI and ML features
              ↓
        MLflow + FastAPI
~~~

## Capabilities

- Kappa-style real-time processing with Kafka and Flink;
- batch processing and feature engineering with PySpark;
- medallion data layers on S3/Delta Lake;
- dbt transformations and analytical marts in Snowflake;
- Airflow orchestration and repeatable workflows;
- MLflow tracking and FastAPI serving for recommendation use cases;
- DataHub lineage/catalogue and Great Expectations data contracts;
- Prometheus, Grafana and Metabase monitoring.

## Repository structure

~~~text
infra/            Terraform and container configuration
ingestion/        Producers, Avro schemas, Airbyte and CDC connectors
data_lake/        Raw, curated and ML feature zones
streaming/        Flink jobs and sinks
batch/            PySpark ingestion and feature engineering
warehouse/        dbt staging, intermediate and marts
orchestration/    Airflow DAGs and operators
ml/               Recommendation training and API serving
governance/       DataHub and Great Expectations
monitoring/       Prometheus and Grafana
~~~

## Quick start

Prerequisites: Docker, Docker Compose, Python and the project dependencies.

~~~bash
make up
pip install -r requirements.txt
make test
~~~

Commands depend on the services enabled in the local environment. Configure secrets through environment variables and never commit credentials.

## Data contracts and quality

The intended contract covers event identity, timestamps, order/customer keys, product attributes and recommendation features. Great Expectations is used to express checks before datasets are promoted between layers.

## Status

Architecture and repository blueprint for a data/ML platform. Validate every service integration and performance claim with reproducible tests before production use.

## Author

**Achraf BRIKI** · [LinkedIn](https://www.linkedin.com/in/achrafbriki/) · [Portfolio](https://achrafbriki.com)
