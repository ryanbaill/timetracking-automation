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
from src.processors.timesheet.entry_processor import TimeEntryProcessor
from src.utils.constants.constants import (
    EXCLUDED_LABEL_IDS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """AWS Lambda handler function."""
    processor = TimeEntryProcessor()
    return processor.process_event(event)

def retry_handler(event, context):
    """AWS Lambda handler for processing SQS retry messages."""
    logger.info("Starting retry handler")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('Timesheet_Entries')
    
    for record in event['Records']:
        message = json.loads(record['body'])
        if message['operation'] == 'write_timesheet_entry':
            table.put_item(Item=message['data'])
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Retry processing complete'})
    }

