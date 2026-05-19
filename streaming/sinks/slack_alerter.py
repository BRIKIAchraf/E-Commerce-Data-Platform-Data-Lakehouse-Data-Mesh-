import json
import requests

class SlackAlerter:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_alert(self, title, message, severity="high"):
        payload = {
            "text": f"🚨 *{title}* [Severity: {severity.upper()}]\\n{message}"
        }
        try:
            # Simulated requests post
            print(f"[Slack Alert Sent] Title: {title} | Message: {message}")
            # requests.post(self.webhook_url, json=payload)
        except Exception as e:
            print(f"Failed to post to Slack: {e}")
