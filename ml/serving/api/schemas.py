from pydantic import BaseModel
from typing import List, Optional

class RecommenderRequest(BaseModel):
    user_id: str
    num_recommendations: Optional[int] = 5

class RecommendationItem(BaseModel):
    product_id: str
    confidence_score: float

class RecommenderResponse(BaseModel):
    user_id: str
    recommendations: List[RecommendationItem]
    serving_mode: str  # e.g., 'hybrid', 'batch_only', 'fallback'
