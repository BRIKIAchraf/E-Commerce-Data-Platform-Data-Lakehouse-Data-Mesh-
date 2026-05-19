# Simple feature store helper to read feature matrices
import pandas as pd

class OfflineFeatureStore:
    def __init__(self, s3_features_path):
        self.path = s3_features_path

    def get_user_features(self, user_ids):
        # In a real implementation this reads Delta/Parquet from S3
        print(f"Querying features from S3 feature store path: {self.path}")
        return pd.DataFrame({
            "user_id": user_ids,
            "total_purchases": [12, 5, 20][:len(user_ids)],
            "lifetime_value": [450.0, 120.0, 1200.0][:len(user_ids)]
        })
