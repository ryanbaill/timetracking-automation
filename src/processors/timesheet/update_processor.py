import json
import logging
import traceback
import boto3
from datetime import datetime
from typing import Dict, Optional, Any
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import EXCLUDED_LABEL_IDS

logger = logging.getLogger(__name__)

# Constants
EXCLUDED_LABEL_IDS = [1111, 2222]  # These are parent labels for tasks included in first service's response that are not needed. The child is what is needed.

logger = logging.getLogger(__name__)

class TimeEntryProcessor:
    """Processes time entry updates from first service to second service sequentially."""
    def __init__(self):
        # Initialize all services needed for processing
        self.config = Config()
        self.first_api = FirstAPI(self.config.API_ONE_TOKEN, self.config.API_ONE_ACCOUNT_ID)
        self.second_api = SecondAPI(
            self.config.API_TWO_ORG_CODE,
            self.config.API_TWO_USERNAME,
            self.config.API_TWO_PASSWORD,
            self.config.API_TWO_USER_ID
        )
        self.dynamodb_handler = DynamoDBHandler()
        self.sns_notifier = SNSNotifier(self.config.SNS_TOPIC_ARN)
        self.sqs = boto3.client('sqs')
        self.queue_url = self.sqs.get_queue_url(QueueName='timesheet-queue')['QueueUrl']

    def process_event(self, event):
        """
        Processes a timesheet entry update event sequentially:
        1. Extract and validate the event data
        2. Get first service timesheet details
        3. Map to second service task
        4. Update in second service
        5. Handle result
        """
        try:
            # Step 1: Extract and validate event data
            logger.info(f"Processing incoming event: {json.dumps(event, indent=2)}")
            body = json.loads(event['body']) if 'body' in event else event

            if not body or 'payload' not in body or 'entity_id' not in body['payload']:
                return self._create_response(400, "Invalid Event", "Missing required payload data")

            payload = body['payload']
            entity_id = payload['entity_id']
            entity_path = payload.get('entity_path', '')

            # Skip AI-generated suggestions
            if 'suggested_hours' in entity_path:
                logger.info(f"Skipping AI-generated hour: {entity_id}")
                return self._create_response(200, "Skipped Entry", "AI-generated suggestion ignored")

            # Step 2: Get first service timesheet details
            logger.info(f"Fetching first service timesheet: {entity_id}")
            event_data = self.first_api.fetch_event(entity_id)
            if event_data is None:
                return self._create_response(200, "Script Aborted", "Deletion flagged as update. Script aborted.")

            # Get valid label ID (excluding parent labels)
            all_label_ids = event_data.get('label_ids', [])
            valid_label_ids = [lid for lid in all_label_ids if lid not in EXCLUDED_LABEL_IDS]

            if not valid_label_ids:
                return self._create_response(200, "Invalid Entry", "No valid label ID found after excluding specified IDs")

            label_id = valid_label_ids[0]

            # Step 3: Map to second service task
            logger.info(f"Mapping first service label {label_id} to second service task")
            task_name = self.dynamodb_handler.get_task_mapping(label_id)
            if not task_name:
                return self._create_response(200, "Mapping Error", f"No task mapping found for label ID: {label_id}")

            # Retrieve existing timesheet entry
            entry_id, external_id = self.dynamodb_handler.get_timesheet_entry(entity_id)
            if entry_id is None:
                return self._create_response(200, "No Entry Found", "No entry ID found. Cannot update timesheet.")

            # Step 4: Update in second service
            # Authenticate with the second API
            app_id = self.second_api.authenticate()

            # Fetch tasks from the second API
            tasks_data = self.second_api.fetch_tasks(app_id, event_data['project']['external_id'])
            if not tasks_data:
                return self._create_response(200, "Fetch Error", "Failed to fetch tasks")

            # Find matching task ID
            task_id = self.find_matching_task_id(tasks_data, task_name)
            if not task_id:
                return self._create_response(200, "Task ID Not Found", f"No matching task ID found for task name: {task_name}")

            # Prepare update data
            parsed_data = self.parse_firstservice_event(event_data)
            parsed_data['user']['external_id'] = external_id

            # Update timesheet
            logger.info("Updating timesheet in second service")
            update_result = self.second_api.update_timesheet(app_id, parsed_data, task_id, entry_id)

            # Step 5: Handle result
            if not update_result['success']:
                error_msg = f"Second service update failed: {update_result.get('error_details')}"
                self.sns_notifier.send_message("Update Error", error_msg)
                return self._create_response(200, "Update Error", error_msg)

            return self._create_response(200, "Update Successful", "The timesheet entry was updated successfully.")

        except Exception as e:
            error_msg = f"Processing error: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self.sns_notifier.send_message("Processing Error", error_msg)
            return self._create_response(500, "Processing Error", error_msg)

    def handle_error(self, error_message, status_code=200):
        """Handle errors and return appropriate response."""
        logger.error(error_message)
        self.sns_notifier.send_message("Update Error", error_message)
        return self._create_response(status_code, "Update Error", error_message)

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

    def find_matching_task_id(self, tasks, task_name):
        """Finds the matching task ID based on the task name."""
        for task in tasks:
            if task.get('strName') == task_name:
                return task.get('idTask')
        logger.warning(f"No matching task ID found for task name: {task_name}")
        return None

    def parse_firstservice_event(self, event_data):
        """Parses the event data from the first service."""
        parsed_data = {
            'client': {
                'external_id': event_data.get('project', {}).get('client', {}).get('external_id')
            },
            'project': {
                'external_id': event_data.get('project', {}).get('external_id')
            },
            'user': {
                'id': event_data.get('user', {}).get('id'),
                'external_id': None  # Will be filled after fetching user data
            },
            'additional_info': {
                'total_hours': event_data.get('duration', 0) / 3600,  # Convert seconds to hours
                'day': event_data.get('additional_info', {}).get('day'),
                'note': event_data.get('note', '')
            }
        }
        return parsed_data
