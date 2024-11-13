import json
import logging
import boto3
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.processors.timesheet.update_processor import TimeEntryProcessor
from src.utils.constants.constants import (
    EXCLUDED_LABEL_IDS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS
)

logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """AWS Lambda handler function for processing updates."""
    logger.info(f"Received webhook data: {json.dumps(event)}")
    processor = TimeEntryProcessor()
    return processor.process_event(event)

def retry_handler(event, context):
    """Handles retry operations from SQS for updates."""
    logger.info("Starting retry handler for updates")
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Timesheet_Entries')

        for record in event['Records']:
            message = json.loads(record['body'])
            if message['operation'] == 'write_timesheet_entry':
                table.put_item(Item=message['data'])
                
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
