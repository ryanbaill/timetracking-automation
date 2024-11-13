import json
import logging
import traceback
from datetime import datetime, timedelta, UTC
from typing import Dict, Optional, Any, List
from boto3.dynamodb.conditions import Attr
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.constants.constants import (
    TABLES,
    SQS_OPERATIONS,
    DEFAULT_TIMEOUT
)

logger = logging.getLogger(__name__)

class CleanupProcessor:
    """Processes database cleanup operations."""
    
    def __init__(self):
        self.config = Config()
        self.dynamodb_handler = DynamoDBHandler()
        self.sns_notifier = SNSNotifier(self.config.SNS_TOPIC_ARN)
        self.sqs_client = SQSClient(self.config.SQS_QUEUE_URL)
        
    def execute_cleanup(self):
        """
        Execute the cleanup process for old timesheet entries.
        Returns the number of entries deleted.
        """
        try:
            # Calculate cutoff date
            retention_days = int(self.config.get_parameter('/db/retention/days'))
            cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
            logger.info(f"Cleaning up entries older than: {cutoff_date}")

            # Scan for old entries
            old_entries = self._scan_old_entries(cutoff_date)
            logger.info(f"Found {len(old_entries)} entries to delete")

            # Process deletions
            deleted_count = self._process_deletions(old_entries)
            
            # Send notification about results
            self._send_notification(deleted_count, cutoff_date)
            
            return deleted_count

        except Exception as e:
            error_message = f"Error in cleanup process: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_message)
            self.sns_notifier.send_message("Cleanup Process Error", error_message)
            raise

    def _scan_old_entries(self, cutoff_date):
        """Scan DynamoDB for entries older than cutoff date."""
        try:
            items = []
            last_evaluated_key = None
            
            while True:
                scan_kwargs = {
                    'FilterExpression': Attr('Date').lt(cutoff_date)
                }
                
                if last_evaluated_key:
                    scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
                    
                response = self.dynamodb_handler.table.scan(**scan_kwargs)
                items.extend(response.get('Items', []))
                
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
                    
            return items
            
        except Exception as e:
            logger.error(f"Error scanning for old entries: {str(e)}")
            raise

    def _process_deletions(self, entries):
        """Process the deletion of entries."""
        deleted_count = 0
        
        for entry in entries:
            try:
                entity_id = entry['FirstServiceEntityID']
                success = self.dynamodb_handler.delete_entry(entity_id)
                
                if success:
                    deleted_count += 1
                    logger.info(f"Deleted entry {entity_id}")
                else:
                    # Queue for retry if deletion fails
                    self.sqs_client.send_message('delete_entry', {'FirstServiceEntityID': entity_id})
                    logger.warning(f"Deletion queued for retry: {entity_id}")
                    
            except Exception as e:
                logger.error(f"Error deleting entry {entry.get('FirstServiceEntityID')}: {str(e)}")
                continue
                
        return deleted_count

    def _send_notification(self, deleted_count, cutoff_date):
        """Send notification about cleanup results."""
        message = (
            f"Successfully deleted {deleted_count} entries older than {cutoff_date}"
            if deleted_count > 0
            else f"No entries found older than {cutoff_date}"
        )
        self.sns_notifier.send_message("Cleanup Process Complete", message)
