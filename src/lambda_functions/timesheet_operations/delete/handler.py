import json
import logging
import boto3
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.processors.timesheet.delete_processor import TimeEntryProcessor
from src.utils.constants.constants import SQS_OPERATIONS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    logger.info(f"Received webhook data: {json.dumps(event)}")
    processor = TimeEntryProcessor()
    try:
        return processor.process_event(event)
    except Exception as e:
        error_message = f"Unhandled error in lambda_handler: {str(e)}"
        stack_trace = traceback.format_exc()
        return processor.handle_error(f"{error_message}\n\nStack Trace:\n{stack_trace}", 500)

def retry_handler(event, context):
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
