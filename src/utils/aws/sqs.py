import boto3
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class SQSClient:
    """Handles interactions with Amazon SQS."""
    def __init__(self, queue_url):
        self.sqs = boto3.client('sqs')
        self.queue_url = queue_url

    def send_message(self, operation, message_data):
        """Sends a message to the SQS queue."""
        try:
            message = {
                'operation': operation,
                'data': message_data
            }
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(message, default=str)
            )
            logger.info(f"Message sent to SQS queue. MessageId: {response.get('MessageId')}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to SQS: {str(e)}")
            return False
