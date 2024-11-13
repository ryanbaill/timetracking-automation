import boto3
import logging
import json
from typing import Optional

# Set up logging
logger = logging.getLogger(__name__)

class SNSNotifier:
    """Handles sending notifications via SNS."""
    def __init__(self, topic_arn):
        self.sns_client = boto3.client('sns')
        self.topic_arn = topic_arn

    def send_message(self, title, description):
        """Sends a message to the configured SNS topic."""
        message = {
            "source": "custom",
            "content": {
                "title": title,
                "description": description
            }
        }
        self.sns_client.publish(TopicArn=self.topic_arn, Message=json.dumps(message))
