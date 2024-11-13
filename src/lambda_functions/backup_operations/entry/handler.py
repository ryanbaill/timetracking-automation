import json
import logging
import traceback
from datetime import datetime
import boto3
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.processors.backup.entry_processor import EventProcessor
from src.utils.apis.first_api import FirstAPI

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

        # Get the entity_id from payload
        payload = body['payload']
        entity_id = payload['entity_id']

        # Initialize First API and fetch full event data
        first_api = FirstAPI()
        event_data = first_api.fetch_event(entity_id)

        if not event_data:
            raise ValueError("Failed to fetch event data from first service")

        # Process the event data
        processor = EventProcessor({"payload": event_data})
        timesheet_data = processor.extract_data()

        # Store in DynamoDB
        db_handler = DynamoDBHandler()
        success = db_handler.write_timesheet_entry(
            firstservice_entity_id=timesheet_data['EntityID'],
            secondservice_entry_id=None,  # Backup entries don't have second service IDs
            firstservice_external_id=None,
            date=datetime.now().strftime('%Y-%m-%d')
        )

        if not success:
            config = Config()
            # If storing in DynamoDB fails, send to SQS for retry
            sqs_client = SQSClient(config.SQS_QUEUE_URL)
            message = {
                'operation': 'store_timesheet',
                'data': timesheet_data
            }
            sqs_success = sqs_client.send_message(message)
            if not sqs_success:
                raise Exception("Failed to send data to SQS queue for retry")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Timesheet backup successful',
                'EntityID': timesheet_data['EntityID']
            })
        }

    except Exception as e:
        error_message = f"Error in lambda_handler: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        config = Config()
        sns_notifier = SNSNotifier(config.SNS_TOPIC_ARN)
        sns_notifier.send_message("Timesheet Backup Error", error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
