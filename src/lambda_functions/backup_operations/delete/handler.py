import json
import logging
import traceback
from datetime import datetime
import boto3

# Local imports
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.apis.first_api import FirstAPI
from src.processors.backup.delete_processor import EventProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """AWS Lambda handler function."""
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")
        
        # Parse the webhook payload
        body = json.loads(event['body']) if 'body' in event else event
        logger.info(f"Parsed body: {json.dumps(body, indent=2)}")

        if not body or 'payload' not in body or 'entity_id' not in body['payload']:
            raise ValueError("Invalid payload structure")

        # Initialize First API and fetch full event data
        first_api = FirstAPI()
        event_data = first_api.fetch_event(body['payload']['entity_id'])

        if not event_data:
            raise ValueError("Failed to fetch event data from first service")

        # Process and validate the event data
        processor = EventProcessor({"payload": event_data})
        validated_data = processor.validate_data()

        # Delete from DynamoDB
        db_handler = DynamoDBHandler()
        success = db_handler.delete_entry(validated_data)

        if not success:
            raise Exception("Failed to delete data in DynamoDB")

        # Simulate fetching the backup entry
        backup_entry, external_id = db_handler.get_backup_entry(validated_data['EntityID'])
        if backup_entry is None:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Backup entry not found'})
            }

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Backup entry deletion successful',
                'EntityID': validated_data['EntityID']
            })
        }

    except ValueError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(e)})
        }
    except Exception as e:
        error_message = f"Error in lambda_handler: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        config = Config()
        sns_notifier = SNSNotifier(config.SNS_TOPIC_ARN)
        sns_notifier.send_message("Backup Entry Deletion Error", error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }

def retry_handler(event, context):
    """AWS Lambda handler for processing SQS retry messages."""
    logger.info("Starting retry handler for deletions")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('Timesheet_Entries')
    
    try:
        for record in event['Records']:
            message = json.loads(record['body'])
            if message['operation'] == 'delete_entry':
                table.delete_item(
                    Key={'FirstServiceEntityID': message['data']['EntityID']}
                )
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Retry deletion successful'})
        }
    except Exception as e:
        logger.error(f"Error in retry handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
