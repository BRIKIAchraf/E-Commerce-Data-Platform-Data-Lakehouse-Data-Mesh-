from fastapi import FastAPI, HTTPException
import os

try:
    from schemas import RecommenderRequest, RecommenderResponse
    from recommender import HybridRecommender
except ImportError:
    from ml.serving.api.schemas import RecommenderRequest, RecommenderResponse
    from ml.serving.api.recommender import HybridRecommender

app = FastAPI(
    title="E-Commerce Recommendation Serving API",
    description="Exposing collaborative filtering and hybrid recommendations via FastAPI.",
    version="1.0.0"
)

# Initialize recommender instance with MLflow Model path
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
recommender = HybridRecommender(mlflow_model_uri=MLFLOW_URI)

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "recommendation-serving-api"}

@app.post("/recommend", response_model=RecommenderResponse)
def get_recommendations(payload: RecommenderRequest):
    try:
        recs = recommender.get_predictions(
            user_id=payload.user_id,
            k=payload.num_recommendations
        )
        return RecommenderResponse(
            user_id=payload.user_id,
            recommendations=recs,
            serving_mode="hybrid"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
