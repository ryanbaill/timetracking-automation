import json
import requests
import logging
import boto3
import traceback
from datetime import datetime

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

    def delete_timesheet(self, app_id, entry_id):
        """Deletes a timesheet entry in the second service."""
        url = f"{self.base_url}/timesheet/?i={self.user_id}&cmd=delete"
        headers = {
            'Cookie': f'appID={app_id}; appOrganization={self.org_code}; appUsername={self.username}'
        }
        data = {
            "idTimesheet": entry_id
        }
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            return {'success': True}
        except Exception as e:
            error_details = f"Failed to delete timesheet: {str(e)}"
            logger.error(error_details)
            return {'success': False, 'error_details': error_details}

class DynamoDBHandler:
    """Handles interactions with DynamoDB."""
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table('Timesheet_Entries')

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
            logger.error(f"Error reading from DynamoDB: {str(e)}")
            return None, None

    def delete_entry(self, firstservice_entity_id):
        """Deletes a timesheet entry from DynamoDB."""
        try:
            self.table.delete_item(
                Key={'FirstServiceEntityID': int(firstservice_entity_id)}
            )
            return True
        except Exception as e:
            logger.error(f"Error deleting from DynamoDB: {str(e)}")
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
    """Processes timesheet deletion events between the first and second services."""
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

            entity_id = webhook_data['payload']['entity_id']
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
            entry_id, _ = self.dynamodb_handler.get_timesheet_entry(entity_id)
            if not entry_id:
                return self.handle_error(f"No matching entry found for Entity ID: {entity_id}")

            # Step 4: Delete the entry from the second service
            app_id = self.second_api.authenticate()
            if not app_id:
                return self.handle_error("Failed to authenticate with second service")

            delete_result = self.second_api.delete_timesheet(app_id, entry_id)
            if not delete_result['success']:
                return self.handle_error(f"Failed to delete timesheet in second service: {delete_result.get('error_details')}")

            # Step 5: Delete the mapping from DynamoDB
            success = self.dynamodb_handler.delete_entry(entity_id)
            if not success:
                # If DynamoDB delete fails, queue it for retry
                self._send_to_queue({
                    'operation': 'delete_entry',
                    'data': {'FirstServiceEntityID': entity_id}
                })

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

def lambda_handler(event, context):
    """AWS Lambda handler function for processing deletions."""
    logger.info(f"Received webhook data: {json.dumps(event)}")
    processor = TimeEntryProcessor()
    try:
        return processor.process_event(event)
    except Exception as e:
        error_message = f"Unhandled error in lambda_handler: {str(e)}"
        stack_trace = traceback.format_exc()
        return processor.handle_error(f"{error_message}\n\nStack Trace:\n{stack_trace}", 500)

def retry_handler(event, context):
    """AWS Lambda handler for processing SQS retry messages."""
    logger.info("Starting retry handler")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('Timesheet_Entries')
    
    try:
        for record in event['Records']:
            message = json.loads(record['body'])
            if message['operation'] == 'delete_entry':
                table.delete_item(Key={'FirstServiceEntityID': message['data']['FirstServiceEntityID']})
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Retry operation successful'})
        }
    except Exception as e:
        logger.error(f"Error in retry handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    
# Main flow of the script:
# 1. Receive Timesheet deletion event data from FirstService API Gateway connected to FirstService webhook
# 2. Parse the event and extract the FirstService entity ID and entity path
# 3. Skip processing if the event is for AI-generated suggestions
# 4. Verify the event exists in FirstService using the entity ID
# 5. Retrieve the corresponding SecondService entry ID from DynamoDB mapping
# 6. Authenticate with SecondService API
# 7. Delete the timesheet entry in SecondService
# 8. Remove the mapping between FirstService and SecondService entries from DynamoDB
# 9. Return appropriate response based on the success or failure of the operation
# 10. If an error occurs, send an SNS message that forwards to a Slack channel to notify about the error
# 11. If DynamoDB deletion fails, queue retry operation to SQS for later processing