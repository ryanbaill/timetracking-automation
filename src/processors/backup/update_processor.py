import json
import logging
import traceback
from datetime import datetime, UTC
from typing import Dict, Optional, Any
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.constants.constants import (
    TABLES,
    SQS_OPERATIONS,
    DEFAULT_TIMEOUT
)

logger = logging.getLogger(__name__)

class EventProcessor:
    """Processes backup update events and extracts relevant data."""
    def __init__(self, event_data):
        if not event_data:
            raise ValueError("Event data cannot be empty")
        self.event_data = event_data

    def extract_data(self):
        """Extracts and structures the required data from the event."""
        try:
            # Parse the event body if it exists
            try:
                body = json.loads(self.event_data.get('body', '{}')) if isinstance(self.event_data.get('body'), str) else self.event_data
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format in event body")
            
            event = body.get('payload')
            if event is None:
                raise ValueError("Missing payload in event data")
            
            if not isinstance(event, dict):
                raise ValueError("Payload must be a dictionary")
            
            if 'id' not in event:
                raise ValueError("Missing required field: id")
            
            today = datetime.now().strftime('%Y-%m-%d')
            
            duration = event.get('duration', {})
            hours = int(duration.get('hours', 0))
            minutes = int(duration.get('minutes', 0))
            
            structured_data = {
                'EntityID': int(event['id']),
                'date_added': today,
                'user_name': event.get('user', {}).get('name', ''),
                'project_name': event.get('project', {}).get('name', ''),
                'client_name': event.get('project', {}).get('client', {}).get('name', ''),
                'hours': hours,
                'minutes': minutes,
                'note': str(event.get('note', '')),
                'label_id': event.get('label_ids', [None])[0],
                'updated_at': int(event.get('updated_at', 0))
            }
            
            logger.info(f"Extracted data: {json.dumps(structured_data, indent=2)}")
            return structured_data
            
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}\n{traceback.format_exc()}")
            raise
