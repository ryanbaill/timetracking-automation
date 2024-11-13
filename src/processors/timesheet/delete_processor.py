import json
import logging
import traceback
import boto3
from datetime import datetime
from typing import Dict, Optional, Any
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI

logger = logging.getLogger(__name__)

class TimeEntryProcessor:
    """Processes timesheet deletion events between the first and second services."""
    def __init__(self):
        # Initialize all services needed for processing
        config = Config()
        self.first_api = FirstAPI(config.API_ONE_TOKEN, config.API_ONE_ACCOUNT_ID)
        self.second_api = SecondAPI(
            config.API_TWO_ORG_CODE,
            config.API_TWO_USERNAME,
            config.API_TWO_PASSWORD,
            config.API_TWO_USER_ID
        )
        self.dynamodb_handler = DynamoDBHandler()
        self.sns_notifier = SNSNotifier(config.SNS_TOPIC_ARN)

    def process_event(self, webhook_data):
        """
        Processes a timesheet deletion event sequentially:
        1. Extract and validate the event data
        2. Verify event exists in the first service
        3. Retrieve the corresponding entry from DynamoDB
        4. Delete the entry from the second service
        5. Delete the mapping from DynamoDB
        6. Handle result
        """
        logger.info(f"Starting deletion workflow with webhook data: {json.dumps(webhook_data)}")
        try:
            # Step 1: Extract and validate event data
            if not webhook_data or 'payload' not in webhook_data or 'entity_id' not in webhook_data['payload']:
                return self.handle_error("Invalid Event: Missing required payload data", 400)

            entity_id = int(webhook_data['payload']['entity_id'])
            entity_path = webhook_data['payload'].get('entity_path', '')

            # Skip AI-generated suggestions if applicable
            if 'suggested_hours' in entity_path:
                logger.info(f"Skipping AI-generated hour deletion for Entity ID: {entity_id}")
                return self._create_response(200, "Skipped Deletion", "AI-generated suggestion ignored")

            # Step 2: Verify event exists in the first service
            logger.info(f"Fetching event from first service with Entity ID: {entity_id}")
            event_data = self.first_api.fetch_event(entity_id)
            if not event_data:
                return self.handle_error(f"Event not found in first service for Entity ID: {entity_id}")

            # Step 3: Retrieve the corresponding entry from DynamoDB
            logger.info(f"Retrieving timesheet entry mapping from DynamoDB for Entity ID: {entity_id}")
            second_service_entry_id, first_service_external_id = self.dynamodb_handler.get_timesheet_entry(entity_id)
            if not second_service_entry_id or not first_service_external_id:
                return self.handle_error(f"No matching entry found for Entity ID: {entity_id}")

            # Step 4: Delete the entry from the second service
            logger.info("Authenticating with second service")
            app_id = self.second_api.authenticate()
            logger.info(f"Deleting timesheet entry in second service with Entry ID: {second_service_entry_id}")
            if not self.second_api.delete_timesheet(app_id, second_service_entry_id):
                return self.handle_error("Failed to delete timesheet in second service")

            # Step 5: Delete the mapping from DynamoDB
            logger.info(f"Deleting DynamoDB entry for Entity ID: {entity_id}")
            if not self.dynamodb_handler.delete_entry(entity_id):
                return self.handle_error("Failed to delete entry from DynamoDB")

            # Step 6: Handle result
            logger.info("Timesheet entry deletion process completed successfully")
            return self._create_response(200, "Deletion Successful", "Timesheet entry deleted successfully")

        except Exception as e:
            error_message = f"Processing error: {str(e)}\n{traceback.format_exc()}"
            return self.handle_error(error_message, 500)

    def handle_error(self, error_message, status_code=200):
        """Handles errors and returns a formatted response."""
        logger.error(error_message)
        self.sns_notifier.send_message("Timesheet Deletion Error", error_message)
        return self._create_response(status_code, "Deletion Error", error_message)

    def _create_response(self, status_code, title, description):
        """Creates a formatted API response."""
        return {
            'statusCode': status_code,
            'body': json.dumps({
                "source": "custom",
                "content": {
                    "title": title,
                    "description": description
                }
            })
        }
