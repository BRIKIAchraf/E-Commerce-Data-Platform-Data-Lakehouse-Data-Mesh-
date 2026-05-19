import json

def test_fraud_rule_high_value():
    from streaming.flink_jobs.fraud_detection import detect_fraud
    
    # 1. High value should trigger alert
    normal_order = json.dumps({"order_id": "ORD-1", "user_id": "U1", "total_amount": 1500.0})
    alert_raw = detect_fraud(normal_order)
    
    assert alert_raw is not None
    alert = json.loads(alert_raw)
    assert alert["alert_type"] == "HIGH_VALUE_TRANSACTION"
    assert alert["severity"] == "CRITICAL"

    # 2. Regular value should not trigger alert
    low_order = json.dumps({"order_id": "ORD-2", "user_id": "U2", "total_amount": 50.0})
    no_alert = detect_fraud(low_order)
    
    assert no_alert is None
