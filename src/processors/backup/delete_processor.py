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
    """Processes backup deletion events and validates data."""
    
    def __init__(self, event_data):
        if not event_data:
            raise ValueError("Event data cannot be empty")
        self.event_data = event_data

    def validate_data(self):
        """Validates the required data from the event."""
        try:
            # Parse the event body if it exists
            try:
                body = json.loads(self.event_data.get('body', '{}')) if isinstance(self.event_data.get('body'), str) else self.event_data
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format in event body")
            
            # Get payload and verify it exists
            event = body.get('payload')
            if event is None:
                raise ValueError("Missing payload in event data")
            
            # Verify payload is not empty and has required fields
            if not isinstance(event, dict):
                raise ValueError("Payload must be a dictionary")
            
            # Check for ID field
            if 'id' not in event:
                raise ValueError("Missing required field: id")
            
            entity_id = int(event['id'])
            logger.info(f"Validated deletion request for EntityID: {entity_id}")
            
            return {'EntityID': entity_id}
            
        except Exception as e:
            logger.error(f"Error validating data: {str(e)}\n{traceback.format_exc()}")
            raise
