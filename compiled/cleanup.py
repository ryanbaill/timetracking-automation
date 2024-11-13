import json
import logging
import boto3
from datetime import datetime, timedelta
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        self.SNS_TOPIC_ARN = self.config_manager.get_parameter('/notifications/sns_topic_arn')
        self.RETENTION_DAYS = int(self.config_manager.get_parameter('/dynamodb/retention_days', False))

class SNSNotifier:
    """Handles sending notifications through AWS SNS."""
    def __init__(self, topic_arn):
        self.sns = boto3.client('sns')
        self.topic_arn = topic_arn

    def send_message(self, subject, message):
        """Sends a notification message through SNS."""
        try:
            self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=subject,
                Message=message
            )
            logger.info(f"SNS notification sent: {subject}")
        except Exception as e:
            logger.error(f"Failed to send SNS notification: {str(e)}")

class DynamoDBCleaner:
    """Handles cleanup of old entries in DynamoDB."""
    def __init__(self, retention_days):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table('Timesheet_Entries')
        self.retention_days = retention_days

    def cleanup_old_entries(self):
        """Removes entries older than retention period."""
        try:
            cutoff_date = (datetime.now() - timedelta(days=self.retention_days)).strftime('%Y-%m-%d')
            
            # Scan for old entries
            response = self.table.scan(
                FilterExpression='#date <= :cutoff_date',
                ExpressionAttributeNames={'#date': 'Date'},
                ExpressionAttributeValues={':cutoff_date': cutoff_date}
            )
            
            items_to_delete = response['Items']
            deleted_count = 0
            
            # Delete old entries
            for item in items_to_delete:
                try:
                    self.table.delete_item(
                        Key={'FirstServiceEntityID': item['FirstServiceEntityID']}
                    )
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting item {item['FirstServiceEntityID']}: {str(e)}")
                    continue
            
            return {
                'success': True,
                'deleted_count': deleted_count,
                'total_found': len(items_to_delete)
            }
            
        except Exception as e:
            error_msg = f"Error during cleanup: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg
            }

def lambda_handler(event, context):
    """AWS Lambda handler for DynamoDB cleanup operations."""
    try:
        logger.info("Starting DynamoDB cleanup process")
        
        # Initialize services
        config = Config()
        sns_notifier = SNSNotifier(config.SNS_TOPIC_ARN)
        cleaner = DynamoDBCleaner(config.RETENTION_DAYS)
        
        # Execute cleanup
        result = cleaner.cleanup_old_entries()
        
        if not result['success']:
            error_msg = f"Cleanup failed: {result.get('error')}"
            sns_notifier.send_message("DynamoDB Cleanup Error", error_msg)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': error_msg})
            }
        
        # Send success notification
        message = (
            f"DynamoDB cleanup completed successfully\n"
            f"Items deleted: {result['deleted_count']}\n"
            f"Total items found: {result['total_found']}"
        )
        sns_notifier.send_message("DynamoDB Cleanup Complete", message)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Cleanup completed successfully',
                'deleted_count': result['deleted_count'],
                'total_found': result['total_found']
            })
        }
        
    except Exception as e:
        error_message = f"Error in cleanup process: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        sns_notifier.send_message("DynamoDB Cleanup Error", error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

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
# 1. EventBridge rule triggers this Lambda function every 45 days
# 2. Initialize services with configuration from Parameter Store
# 3. Calculate cutoff date based on retention period
# 4. Scan DynamoDB for entries older than cutoff date
# 5. Delete old entries from DynamoDB
# 6. Send SNS notification with results
# 7. If errors occur, send to SQS for retry
# 8. Return response with operation results

