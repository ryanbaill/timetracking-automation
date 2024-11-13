import json
import logging
import requests
import boto3
import traceback
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConfigManager:
    """Manages configuration retrieval from AWS Systems Manager Parameter Store."""
    def __init__(self):
        self.ssm = boto3.client('ssm', region_name='us-east-1')
        self._config = {}

    def get_parameter(self, param_name, decrypt=True):
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
        self.API_ONE_TOKEN = self.config_manager.get_parameter('/api/firstservice/token')
        self.API_ONE_ACCOUNT_ID = self.config_manager.get_parameter('/api/firstservice/account_id')
        self.SNS_TOPIC_ARN = self.config_manager.get_parameter('/notifications/sns_topic_arn')
        self.SQS_QUEUE_URL = self.config_manager.get_parameter('/sqs/queue_url')

class FirstAPI:
    """Handles interactions with the first service API."""
    def __init__(self, token, account_id):
        self.token = token
        self.account_id = account_id
        self.base_url = "https://api.service1.com/1.1"
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

    def fetch_event(self, entity_id):
        """Fetches event details from first service."""
        url = f"{self.base_url}/{self.account_id}/events/{entity_id}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching event: {str(e)}")
            return None

class SNSNotifier:
    """Handles sending notifications through AWS SNS."""
    def __init__(self, topic_arn):
        self.sns = boto3.client('sns')
        self.topic_arn = topic_arn

    def send_message(self, subject, message):
        try:
            self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=subject,
                Message=message
            )
            logger.info(f"SNS notification sent: {subject}")
        except Exception as e:
            logger.error(f"Failed to send SNS notification: {str(e)}")

class DynamoDBHandler:
    """Handles interactions with DynamoDB backup table."""
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table('Timesheet_Backup')

    def update_backup_entry(self, entity_id, event_data):
        """Updates a timesheet entry in the backup table."""
        try:
            self.table.update_item(
                Key={'FirstServiceEntityID': int(entity_id)},
                UpdateExpression='SET EventData = :ed, LastModified = :lm',
                ExpressionAttributeValues={
                    ':ed': event_data,
                    ':lm': datetime.now().isoformat()
                }
            )
            return True
        except Exception as e:
            logger.error(f"Error updating DynamoDB: {str(e)}")
            return False

class BackupProcessor:
    """Processes timesheet entry updates for backup."""
    def __init__(self):
        self.config = Config()
        self.first_api = FirstAPI(self.config.API_ONE_TOKEN, self.config.API_ONE_ACCOUNT_ID)
        self.dynamodb_handler = DynamoDBHandler()
        self.sns_notifier = SNSNotifier(self.config.SNS_TOPIC_ARN)
        self.sqs = boto3.client('sqs')
        self.queue_url = self.config.SQS_QUEUE_URL

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

    def process_event(self, event):
        """
        Processes a timesheet entry update for backup:
        1. Extract and validate the event data
        2. Fetch updated event details from first service
        3. Update in backup table
        """
        try:
            # Step 1: Extract and validate event data
            logger.info(f"Processing incoming event: {json.dumps(event, indent=2)}")
            body = json.loads(event['body']) if 'body' in event else event

            if not body or 'payload' not in body or 'entity_id' not in body['payload']:
                return self._create_response(400, "Invalid Event", "Missing required payload data")

            entity_id = body['payload']['entity_id']
            entity_path = body['payload'].get('entity_path', '')

            # Skip AI-generated suggestions
            if 'suggested_hours' in entity_path:
                logger.info(f"Skipping AI-generated hour: {entity_id}")
                return self._create_response(200, "Skipped Entry", "AI-generated suggestion ignored")

            # Step 2: Fetch updated event details
            event_data = self.first_api.fetch_event(entity_id)
            if not event_data:
                return self._create_response(500, "Fetch Error", "Failed to fetch event details")

            # Step 3: Update in backup table
            success = self.dynamodb_handler.update_backup_entry(entity_id, event_data)
            if not success:
                self._send_to_queue({
                    'operation': 'update_backup_entry',
                    'data': {
                        'Key': {'FirstServiceEntityID': int(entity_id)},
                        'UpdateExpression': 'SET EventData = :ed, LastModified = :lm',
                        'ExpressionAttributeValues': {
                            ':ed': event_data,
                            ':lm': datetime.now().isoformat()
                        }
                    }
                })
                error_msg = "Failed to update backup entry"
                self.sns_notifier.send_message("Backup Update Error", error_msg)
                return self._create_response(500, "Backup Error", error_msg)

            return self._create_response(200, "Success", "Entry updated successfully")

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
                'title': title,
                'description': description
            })
        }

def lambda_handler(event, context):
    """AWS Lambda handler function for backup update processing."""
    logger.info(f"Received webhook data: {json.dumps(event)}")
    processor = BackupProcessor()
    return processor.process_event(event)

def retry_handler(event, context):
    """AWS Lambda handler for processing SQS retry messages."""
    logger.info("Starting retry handler")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('Timesheet_Backup')
    
    try:
        for record in event['Records']:
            message = json.loads(record['body'])
            if message['operation'] == 'update_backup_entry':
                table.update_item(**message['data'])
        
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
# 1. Receive timesheet update event data from FirstService API Gateway webhook
# 2. Extract and validate the event data (entity_id and entity_path)
# 3. Skip processing if the event is for AI-generated suggestions
# 4. Fetch updated event details from FirstService using the entity ID
# 5. Update the timesheet entry in DynamoDB backup table with new data
# 6. If DynamoDB update fails, queue retry operation to SQS
# 7. Send SNS notification if any errors occur
# 8. Return response with operation results