import pytest
from fastapi.testclient import TestClient

def test_api_health():
    from ml.serving.api.main import app
    client = TestClient(app)
    
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_recommendations_endpoint():
    from ml.serving.api.main import app
    client = TestClient(app)
    
    payload = {"user_id": "USR-101", "num_recommendations": 3}
    response = client.post("/recommend", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "USR-101"
    assert len(data["recommendations"]) == 3
    assert data["serving_mode"] == "hybrid"
