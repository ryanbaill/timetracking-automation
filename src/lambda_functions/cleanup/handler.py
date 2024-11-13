import json
import logging
import traceback
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any
from src.utils.config.config_manager import Config
from src.utils.aws.sns import SNSNotifier
from src.processors.cleanup.cleanup_processor import CleanupProcessor
from src.utils.constants.constants import (
    TABLES,
    SQS_OPERATIONS
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """AWS Lambda handler for database cleanup operations."""
    try:
        logger.info("Starting cleanup process")
        
        # Initialize configuration and processor
        config = Config()
        processor = CleanupProcessor()
        sns_notifier = SNSNotifier(config.SNS_TOPIC_ARN)
        
        # Execute cleanup
        deleted_count = processor.execute_cleanup()
        
        # Create success message
        if deleted_count > 0:
            message = f"Successfully deleted {deleted_count} entries"
        else:
            message = "No entries found for deletion"
            
        logger.info(message)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': message,
                'deleted_count': deleted_count
            })
        }

    except Exception as e:
        error_message = f"Cleanup process failed: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        sns_notifier.send_message("Cleanup Process Error", error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

def retry_handler(event, context):
    """AWS Lambda handler for processing SQS retry messages."""
    logger.info("Starting retry handler for cleanup operations")
    
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Timesheet_Entries')
        
        for record in event['Records']:
            message = json.loads(record['body'])
            if message['operation'] == 'delete_entry':
                table.delete_item(
                    Key={'FirstServiceEntityID': message['data']['FirstServiceEntityID']}
                )
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Retry cleanup successful'})
        }
    except Exception as e:
        error_message = f"Error in retry handler: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
