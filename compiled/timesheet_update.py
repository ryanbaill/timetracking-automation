import json
import requests
import logging
import boto3
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
EXCLUDED_LABEL_IDS = [1111, 2222]  # Parent labels to exclude

class ConfigManager:
    """Manages configuration retrieval from AWS Systems Manager Parameter Store."""
    def __init__(self):
        self.ssm = boto3.client('ssm', region_name='us-east-1')
        self._config = {}

    def get_parameter(self, param_name, decrypt=True):
        """Retrieves a parameter from SSM Parameter Store."""
        if param_name not in self._config:
            try:
                response = self.ssm.get_parameter(
                    Name=param_name,
                    WithDecryption=decrypt
                )
                self._config[param_name] = response['Parameter']['Value']
            except Exception as e:
                logger.error(f"Error fetching parameter {param_name}: {str(e)}")
                raise
        return self._config[param_name]

class Config:
    """Configuration constants retrieved from Parameter Store."""
    def __init__(self):
        self.config_manager = ConfigManager()
        
        # API One parameters
        self.API_ONE_TOKEN = self.config_manager.get_parameter('/api/firstservice/token')
        self.API_ONE_ACCOUNT_ID = self.config_manager.get_parameter('/api/firstservice/account_id')
        
        # API Two parameters
        self.API_TWO_ORG_CODE = self.config_manager.get_parameter('/api/secondservice/org_code')
        self.API_TWO_USERNAME = self.config_manager.get_parameter('/api/secondservice/username')
        self.API_TWO_PASSWORD = self.config_manager.get_parameter('/api/secondservice/password')
        self.API_TWO_USER_ID = self.config_manager.get_parameter('/api/secondservice/user_id')
        
        # SNS configuration
        self.SNS_TOPIC_ARN = self.config_manager.get_parameter('/notifications/sns_topic_arn')

class FirstAPI:
    """Handles interactions with the first time tracking API."""
    def __init__(self, token, account_id):
        self.token = token
        self.account_id = account_id
        self.base_url = "https://api.service1.com/1.1"

    def fetch_event(self, entity_id):
        """Fetches event using the given entity ID."""
        url = f"{self.base_url}/{self.account_id}/events/{entity_id}"
        headers = {
            'Authorization': f'Bearer {self.token}',
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching event: {str(e)}")
            return None

    def fetch_user(self, user_id):
        """Fetches user details from the first service."""
        url = f"{self.base_url}/{self.account_id}/users/{user_id}"
        headers = {
            'Authorization': f'Bearer {self.token}',
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching user: {str(e)}")
            return None

class SecondAPI:
    """Handles interactions with the second time tracking API."""
    def __init__(self, org_code, username, password, user_id):
        self.org_code = org_code
        self.username = username
        self.password = password
        self.user_id = user_id
        self.base_url = "https://api.service2.com/service/api"

    def authenticate(self):
        """Authenticates with second service and returns the app ID."""
        auth_url = f"{self.base_url}/login/"
        auth_data = {
            "cmd": "org",
            "idOrg": self.org_code,
            "strUsername": self.username,
            "strPassword": self.password
        }
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        try:
            response = requests.post(auth_url, data=auth_data, headers=headers)
            response.raise_for_status()
            auth_data = response.json()
            if 'appID' not in auth_data:
                raise Exception("Authentication failed: appID not found")
            return auth_data['appID']
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return None

    def fetch_tasks(self, app_id, job_id):
        """Fetches tasks from the second service."""
        url = f"{self.base_url}/Task/?i={self.user_id}&cmd=list&idJob={job_id}"
        headers = {
            'Cookie': f'appID={app_id}; appOrganization={self.org_code}; appUsername={self.username}'
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get('listTasks')
        except Exception as e:
            logger.error(f"Error fetching tasks: {str(e)}")
            return None

    def update_timesheet(self, app_id, parsed_data, task_id, entry_id):
        """Updates a timesheet entry in the second service."""
        url = f"{self.base_url}/timesheet/?i={self.user_id}&cmd=update"
        data = {
            "idTimesheet": entry_id,
            "idClient": parsed_data['client']['external_id'],
            "idJob": parsed_data['project']['external_id'],
            "idTask": task_id,
            "idPersonnel": parsed_data['user']['external_id'],
            "dblHours": parsed_data['additional_info']['total_hours'],
            "dtTimesheet": parsed_data['additional_info']['day'],
            "strDescription": parsed_data['additional_info']['note']
        }
        headers = {'Cookie': f'appID={app_id}; appOrganization={self.org_code}; appUsername={self.username}'}
        try:
            response = requests.post(url, data=data, headers=headers)
            response_data = response.json()
            if response.status_code == 200 and 'error' not in response_data:
                return {"success": True, "message": "Timesheet updated successfully."}
            else:
                return {"success": False, "message": "Failed to update timesheet.", "error_details": response_data}
        except Exception as e:
            logger.error(f"Error updating timesheet: {str(e)}")
            return {"success": False, "message": "Failed to update timesheet.", "error_details": str(e)}

class DynamoDBHandler:
    """Handles interactions with DynamoDB."""
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table('Timesheet_Entries')
        self.mapping_table = self.dynamodb.Table('Label_Task_Mapping')

    def get_task_mapping(self, firstservice_label_id):
        """Retrieves second service's task name for a first service label ID."""
        try:
            response = self.mapping_table.get_item(
                Key={'FirstServiceLabelID': int(firstservice_label_id)}
            )
            return response.get('Item', {}).get('SecondServiceTask')
        except Exception as e:
            logger.error(f"Error in get_task_mapping: {str(e)}")
            return None

    def get_timesheet_entry(self, firstservice_entity_id):
        """Retrieves timesheet entry mapping from DynamoDB."""
        try:
            response = self.table.get_item(
                Key={'FirstServiceEntityID': int(firstservice_entity_id)}
            )
            if 'Item' in response:
                return response['Item'].get('SecondServiceEntryID'), response['Item'].get('FirstServiceExternalID')
            return None, None
        except Exception as e:
            logger.error(f"Error in get_timesheet_entry: {str(e)}")
            return None, None

    def write_timesheet_entry(self, entity_id, entry_id, external_id, date):
        """Writes a timesheet entry to DynamoDB."""
        try:
            item = {
                'FirstServiceEntityID': int(entity_id),
                'SecondServiceEntryID': int(entry_id),
                'FirstServiceExternalID': external_id,
                'Date': date
            }
            self.table.put_item(Item=item)
            return True
        except Exception as e:
            logger.error(f"Error writing timesheet entry: {str(e)}")
            return False

class SNSNotifier:
    """Handles sending notifications via SNS."""
    def __init__(self, topic_arn):
        self.sns_client = boto3.client('sns')
        self.topic_arn = topic_arn

    def send_message(self, title, description):
        """Sends a message to the configured SNS topic."""
        try:
            message = {
                "source": "custom",
                "content": {
                    "title": title,
                    "description": description
                }
            }
            self.sns_client.publish(
                TopicArn=self.topic_arn,
                Message=json.dumps(message)
            )
        except Exception as e:
            logger.error(f"Error sending SNS message: {str(e)}")

class TimeEntryProcessor:
    """Processes time entry updates from first service to second service sequentially."""
    def __init__(self):
        # Initialize services
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
        5. Update DynamoDB mapping
        6. Handle result
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
            if not app_id:
                return self._create_response(500, "Auth Error", "Failed to authenticate with second service")

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

            # Step 5: Handle result and save mapping
            if not update_result['success']:
                error_msg = f"Second service update failed: {update_result.get('error_details')}"
                self.sns_notifier.send_message("Update Error", error_msg)
                return self._create_response(200, "Update Error", error_msg)

            # Save the mapping
            success = self.dynamodb_handler.write_timesheet_entry(
                entity_id,
                entry_id,  # Using existing entry_id since it's an update
                parsed_data['user']['external_id'],
                parsed_data['additional_info']['day']
            )

            if not success:
                # If DynamoDB update fails, queue it for retry
                self._send_to_queue({
                    'operation': 'write_timesheet_entry',
                    'data': {
                        'FirstServiceEntityID': entity_id,
                        'SecondServiceEntryID': entry_id,
                        'FirstServiceExternalID': parsed_data['user']['external_id'],
                        'Date': parsed_data['additional_info']['day']
                    }
                })
                logger.warning("DynamoDB update queued for retry")

            return self._create_response(200, "Update Successful", "The timesheet entry was updated successfully.")

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

    def find_matching_task_id(self, tasks, task_name):
        """Finds the matching task ID based on the task name."""
        for task in tasks:
            if task.get('strName') == task_name:
                return task.get('idTask')
        logger.warning(f"No matching task ID found for task name: {task_name}")
        return None

    def parse_firstservice_event(self, event_data):
        """Parses the event data from the first service."""
        return {
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
                'day': datetime.utcfromtimestamp(event_data.get('timestamp')).strftime('%Y-%m-%d'),
                'note': event_data.get('note', '')
            }
        }

    def _send_to_queue(self, message):
        """Sends a message to the SQS queue."""
        try:
            self.sqs.send_message(
                QueueUrl=self.queue_url,
               MessageBody=json.dumps(message)
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send message to SQS: {str(e)}")
            return False

    def handle_error(self, error_message, status_code=200):
        """Handle errors and return appropriate response."""
        logger.error(error_message)
        self.sns_notifier.send_message("Update Error", error_message)
        return self._create_response(status_code, "Update Error", error_message)

def lambda_handler(event, context):
   """AWS Lambda handler function for processing updates."""
   logger.info(f"Received webhook data: {json.dumps(event)}")
   processor = TimeEntryProcessor()
   return processor.process_event(event)

def retry_handler(event, context):
   """AWS Lambda handler for processing SQS retry messages."""
   logger.info("Starting retry handler")
   dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
   table = dynamodb.Table('Timesheet_Entries')
   
   try:
       for record in event['Records']:
           message = json.loads(record['body'])
           if message['operation'] == 'write_timesheet_entry':
               table.put_item(Item=message['data'])
       
       return {
           'statusCode': 200,
           'body': json.dumps({
               'status': 'success',
               'message': 'Retry operation successful'
           })
       }
   except Exception as e:
       logger.error(f"Error in retry handler: {str(e)}")
       return {
           'statusCode': 500,
           'body': json.dumps({
               'status': 'error',
               'error': str(e)
           })
       }

# Main flow of the script:
# 1. Receive Timesheet update event data from FirstService API Gateway connected to FirstService webhook
# 2. Parse the event and extract the FirstService entity ID
# 3. Fetch FirstService event data using the entity ID
# 4. Process the FirstService event data and extract the relevant label ID
# 5. Retrieve the corresponding SecondService entry ID and FirstService external ID from DynamoDB
# 6. Authenticate with SecondService API
# 7. Retrieve the SecondService task based on the FirstService label ID
# 8. Fetch SecondService tasks for the given job ID
# 9. Find the matching SecondService task ID based on the task name
# 10. Update the timesheet entry in SecondService
# 11. Return appropriate response based on the success or failure of the operation
# 12. If an error occurs, send an SNS message that forwards to a Slack channel to notify about the error
# 13. If error occurs, retry operation is queued to SQS