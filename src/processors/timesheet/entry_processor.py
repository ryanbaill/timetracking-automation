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

# Set up logging
logger = logging.getLogger(__name__)

# Constants
EXCLUDED_LABEL_IDS = [1111, 2222] # These are parent labels for tasks included in first service's response that are not needed. The child is what is needed.

class TimeEntryProcessor:
    """Processes time entries from first service to second service sequentially."""
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
        Processes a timesheet entry event sequentially:
        1. Extract and validate the event data
        2. Get first service timesheet details
        3. Map to second service task
        4. Submit to second service
        5. Save the mapping
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
            firstservice_events = self.first_api.fetch_events(entity_id)
            
            # Get valid label ID (excluding parent labels)
            all_label_ids = firstservice_events.get('label_ids', [])
            valid_label_ids = [lid for lid in all_label_ids if lid not in EXCLUDED_LABEL_IDS]
            
            if not valid_label_ids:
                return self._create_response(200, "Invalid Entry", "No valid label ID found")
            
            label_id = valid_label_ids[0]

            # Step 3: Map to second service task
            logger.info(f"Mapping first service label {label_id} to second service task")
            secondservice_task = self.dynamodb_handler.get_second_task(label_id)
            
            if not secondservice_task:
                return self._create_response(200, "Mapping Error", 
                    f"No second service task found for label {label_id}")

            # Step 4: Submit to second service
            # First authenticate
            app_id = self.second_api.authenticate()
            
            # Get job details
            job_id = firstservice_events.get('project', {}).get('external_id')
            secondservice_tasks = self.second_api.fetch_tasks(app_id, job_id)
            task_id = self.find_matching_task_id(secondservice_tasks, secondservice_task)

            # Prepare submission data
            parsed_data = self.parse_firstservice_event(firstservice_events)
            user_data = self.first_api.fetch_user(parsed_data['user']['id'])
            parsed_data['user']['external_id'] = user_data.get('external_id') if user_data else None

            # Submit timesheet
            logger.info("Submitting timesheet to second service")
            submit_result = self.second_api.submit_timesheet(app_id, parsed_data, task_id)

            # Step 5: Handle result and save mapping
            if not submit_result['success']:
                error_msg = f"Second service submission failed: {submit_result.get('error_details')}"
                self.sns_notifier.send_message("Submission Error", error_msg)
                return self._create_response(200, "Submission Error", error_msg)

            # Save the mapping
            secondservice_entry_id = submit_result['idTimesheet']
            date = parsed_data['additional_info']['day']
            user_external_id = parsed_data['user']['external_id']
            
            self.dynamodb_handler.write_timesheet_entry(
                entity_id,
                secondservice_entry_id,
                user_external_id,
                date
            )

            return self._create_response(200, "Success", "Timesheet processed successfully")

        except Exception as e:
            error_msg = f"Processing error: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self.sns_notifier.send_message("Processing Error", error_msg)
            return self._create_response(500, "Processing Error", error_msg)

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
