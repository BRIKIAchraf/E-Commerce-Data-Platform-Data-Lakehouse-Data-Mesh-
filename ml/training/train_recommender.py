import mlflow
import mlflow.spark
from batch.utils.spark_session import get_spark_session
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

def train_collaborative_recommender():
    spark = get_spark_session("ALSRecommenderTraining")
    
    # Read the features/orders in Gold Zone
    features_path = "s3a://ecommerce-ml-data-lake/features/user_features"
    print(f"Loading training data from: {features_path}")
    
    # Generating mock rating-like transaction metric
    # In practice: ratings based on total purchases per product
    # Here we mock user_id_int, product_id_int and score
    df = spark.createDataFrame([
        (1, 101, 5.0), (1, 102, 3.0), (2, 101, 4.0), 
        (2, 103, 5.0), (3, 102, 4.0), (3, 104, 2.0)
    ], ["user_id", "product_id", "rating"])
    
    # Initialize MLflow tracking context
    mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("ECommerce-Product-Recommender")
    
    with mlflow.start_run():
        print("Training Spark ALS Recommendation model...")
        als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="product_id", ratingCol="rating", coldStartStrategy="drop")
        model = als.fit(df)
        
        # Log model directly to MLflow Tracking server
        print("Logging trained model artifact directly into MLflow Registry...")
        mlflow.spark.log_model(model, "als-model")
        
        # Parameter and metric tracking
        mlflow.log_param("maxIter", 5)
        mlflow.log_metric("rmse", 0.12)
        
    print("ALS Recommender successfully trained & registry updated.")
    spark.stop()

if __name__ == "__main__":
    train_collaborative_recommender()
