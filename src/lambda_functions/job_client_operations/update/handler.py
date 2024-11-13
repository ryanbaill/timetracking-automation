import json
import logging
import traceback
import boto3
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.processors.job_client.update_processor import JobUpdateProcessor
from src.utils.constants.constants import (
    EXCLUDED_CLIENTS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS,
    TABLES
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """AWS Lambda handler for client/project updates and deletions."""
    try:
        logger.info(f"Starting update process with event: {json.dumps(event)}")
        
        # Initialize services
        config = Config()
        first_api = FirstAPI(config.API_ONE_TOKEN, config.API_ONE_ACCOUNT_ID)
        second_api = SecondAPI(
            config.API_TWO_ORG_CODE,
            config.API_TWO_USERNAME,
            config.API_TWO_PASSWORD,
            config.API_TWO_USER_ID
        )
        
        # Initialize handlers
        dynamodb_handler = DynamoDBHandler()
        sns_notifier = SNSNotifier(config.SNS_TOPIC_ARN)
        sqs_client = SQSClient(config.SQS_QUEUE_URL)

        # Initialize processor and process changes
        processor = JobUpdateProcessor(
            first_api, 
            second_api, 
            dynamodb_handler, 
            sns_notifier,
            sqs_client
        )
        
        result = processor.process_changes()
        
        if not result['success']:
            raise Exception(f"Update process failed: {result.get('error')}")
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Update process completed successfully',
                'updates': result['updated'],
                'deletions': result['deleted'],
                'orphaned': result['orphaned']
            })
        }

    except Exception as e:
        error_message = f"Update process failed: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        sns_notifier.send_message("Job Update Error", error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

def retry_handler(event, context):
    """Processes retry operations from SQS."""
    logger.info("Starting retry handler")
    
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Job_Entries')
        
        for record in event['Records']:
            message = json.loads(record['body'])
            operation = message['operation']
            data = message['data']
            
            if operation == 'update_job':
                table.put_item(Item=data)
            elif operation == 'delete_job':
                table.delete_item(Key={'JobID': data['JobID']})
            
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Retry operations completed successfully'})
        }
    except Exception as e:
        error_message = f"Retry handler failed: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
