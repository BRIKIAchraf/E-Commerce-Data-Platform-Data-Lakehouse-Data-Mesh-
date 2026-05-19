# E-Commerce Data Platform (Data Lakehouse & Data Mesh)

Bienvenue sur le dépôt de la plateforme Data E-Commerce. Ce projet fournit une architecture moderne intégrant le traitement en temps réel (Kappa via Flink) et le traitement batch (Lakehouse/ELT via Spark, dbt et Snowflake) avec des garanties fortes de gouvernance, qualité et suivi ML.

## 🛠️ Architecture Technologique
* **Streaming (Kappa)** : Kafka $\\rightarrow$ Flink $\\rightarrow$ S3 & Snowflake
* **Lakehouse (Batch)** : S3 (Delta Lake) $\\rightarrow$ PySpark $\\rightarrow$ Snowflake
* **Transformation (ELT)** : dbt Core sur Snowflake (Bronze $\\rightarrow$ Silver $\\rightarrow$ Gold)
* **Orchestration** : Apache Airflow
* **Serving ML** : MLflow + FastAPI (Recommandations personnalisées)
* **Gouvernance & Qualité** : DataHub (Lignée & Catalogue) + Great Expectations (Data Contracts)
* **Monitoring** : Prometheus + Grafana + Metabase

## 📁 Structure du Projet
Le dépôt est organisé de manière modulaire, reflétant chaque couche fonctionnelle :
* `infra/` : Fichiers Terraform d'infrastructure as code et Dockerfiles.
* `ingestion/` : Kafka Producers, schémas Avro et connecteurs Airbyte / Debezium CDC.
* `data_lake/` : Emplacement logique des zones S3 Raw, Curated (Medallion) et ML features.
* `streaming/` : Flink stream jobs et Sinks pour l'analyse en temps réel.
* `batch/` : PySpark Jobs pour l'ingestion Bronze/Silver et le Feature Engineering.
* `warehouse/` : Projet d'analyse d'entrepôt de données dbt (staging, intermediate, marts).
* `orchestration/` : DAGs Airflow et opérateurs customisés.
* `ml/` : Entraînement de recommandations via Spark MLlib et API FastAPI.
* `governance/` : Scripts DataHub pour le lignage et suites Great Expectations.
* `monitoring/` : Configuration de Prometheus et dashboards Grafana.

## 🚀 Démarrage Rapide

1. Instancier le Stack local :
   ```bash
   make up
   ```
2. Installer les dépendances Python localement :
   ```bash
   pip install -r requirements.txt
   ```
3. Lancer les tests unitaires :
   ```bash
   make test
   ```
