import json
import logging
import boto3
import traceback
from datetime import datetime, UTC
from typing import Dict, Any, Optional
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.processors.job_client.entry_processor import JobProcessor
from src.utils.constants.constants import (
    EXCLUDED_CLIENTS,
    DEFAULT_PROJECT_COLOR,
    DEFAULT_RATE_TYPE,
    DEFAULT_USER_IDS,
    DEFAULT_LABEL_IDS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS,
    TABLES
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """AWS Lambda handler for client/project synchronization."""
    try:
        logger.info(f"Starting synchronization with event: {json.dumps(event)}")
        
        # Initialize configuration and services
        config = Config()
        first_api = FirstAPI(config.API_ONE_TOKEN, config.API_ONE_ACCOUNT_ID)
        second_api = SecondAPI(
            config.API_TWO_ORG_CODE,
            config.API_TWO_USERNAME,
            config.API_TWO_PASSWORD,
            config.API_TWO_USER_ID
        )
        
        # Initialize notification services
        sns_notifier = SNSNotifier(config.SNS_TOPIC_ARN)
        sqs_client = SQSClient(config.SQS_QUEUE_URL)

        # Authenticate with second service
        second_api.authenticate()
        
        # Initialize processor
        processor = JobProcessor(first_api, second_api)
        
        # Process clients
        client_sync = processor.process_clients()
        if not client_sync['success']:
            raise Exception(f"Client sync failed: {client_sync.get('error')}")
        
        # Process projects
        project_sync = processor.process_projects(client_sync['clients'])
        if not project_sync['success']:
            raise Exception(f"Project sync failed: {project_sync.get('error')}")

        # Handle any failed operations by sending to SQS
        for result in client_sync['results']:
            if not result.get('created') and 'retry_data' in result:
                sqs_client.send_message('create_client', result['retry_data'])

        for result in project_sync['results']:
            if not result.get('created') and 'retry_data' in result:
                sqs_client.send_message('create_project', result['retry_data'])
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Synchronization completed successfully',
                'clients': client_sync['results'],
                'projects': project_sync['results']
            })
        }

    except Exception as e:
        error_message = f"Synchronization failed: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        sns_notifier.send_message("Job Synchronization Error", error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

def retry_handler(event, context):
    """Processes retry operations from SQS."""
    logger.info("Starting retry handler")
    try:
        config = Config()
        first_api = FirstAPI(config.API_ONE_TOKEN, config.API_ONE_ACCOUNT_ID)
        
        for record in event['Records']:
            message = json.loads(record['body'])
            operation = message['operation']
            data = message['data']
            
            if operation == 'create_client':
                first_api.create_client(data)
            elif operation == 'create_project':
                first_api.create_project(data)
            
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
