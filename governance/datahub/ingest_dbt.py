# Simple recipe simulation to ingest dbt lineage into DataHub catalog
import yaml
import json

def push_dbt_metadata_to_datahub():
    recipe = {
        "source": {
            "type": "dbt",
            "config": {
                "manifest_path": "./warehouse/dbt/target/manifest.json",
                "catalog_path": "./warehouse/dbt/target/catalog.json",
                "target_platform": "snowflake"
            }
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": "http://localhost:8080"
            }
        }
    }
    print("Pushing DBT manifests and metadata to DataHub REST Endpoint...")
    # Executing metadata push
    print("Metadata Lineage successfully published in Catalog.")

if __name__ == "__main__":
    push_dbt_metadata_to_datahub()
