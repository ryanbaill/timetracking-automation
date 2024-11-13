import boto3
import logging
import json
from datetime import datetime
from typing import Dict, Optional, Any, Tuple
from src.utils.config.config_manager import Config
from src.utils.aws.sqs import SQSClient
from src.utils.constants.constants import (
    TABLES,
    SQS_OPERATIONS
)

logger = logging.getLogger(__name__)

class DynamoDBHandler:
    """Handles interactions with DynamoDB."""

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.Table('Timesheet_Entries')
        config = Config()
        self.sqs_client = SQSClient(config.SQS_QUEUE_URL)

    def get_second_task(self, firstservice_label_id):
        """Retrieves second service's task associated with first service's label ID."""
        table = self.dynamodb.Table('FirstService_Labels_SecondService_Task_IDs')
        logger.info(f"Querying DynamoDB table for FirstServiceLabelID: {firstservice_label_id}")
        try:
            response = table.get_item(Key={'FirstServiceLabelID': int(firstservice_label_id)})
            item = response.get('Item')
            if item and 'SecondServiceTask' in item:
                logger.info(f"SecondServiceTask found: {item['SecondServiceTask']}")
                return item['SecondServiceTask']
            else:
                logger.warning(f"No SecondServiceTask found for FirstServiceLabelID: {firstservice_label_id}")
                return None
        except Exception as e:
            logger.error(f"Error in get_second_task: {str(e)}")
            return None

    def write_timesheet_entry(self, firstservice_entity_id, secondservice_entry_id, firstservice_external_id, date=None):
        """Writes a timesheet entry mapping to DynamoDB."""
        try:
            item = {
                'FirstServiceEntityID': int(firstservice_entity_id),
                'SecondServiceEntryID': int(secondservice_entry_id) if secondservice_entry_id else None,
                'FirstServiceExternalID': int(firstservice_external_id) if firstservice_external_id else None,
                'Date': date if date else datetime.now().strftime('%Y-%m-%d')
            }
            self.table.put_item(Item=item)
            return True
        except Exception as e:
            logger.error(f"Error writing to DynamoDB: {str(e)}")
            return False

    def get_timesheet_entry(self, firstservice_entity_id):
        """Retrieve existing timesheet entry mapping."""
        try:
            response = self.table.get_item(
                Key={'FirstServiceEntityID': int(firstservice_entity_id)}
            )
            if 'Item' in response:
                return response['Item'].get('SecondServiceEntryID'), response['Item'].get('FirstServiceExternalID')
            return None, None
        except Exception as e:
            logger.error(f"Error reading from DynamoDB: {str(e)}")
            return None, None

    def get_task_mapping(self, label_id):
        """Retrieve task mapping from DynamoDB."""
        mapping_table = self.dynamodb.Table('FirstService_Labels_SecondService_Task_IDs')
        try:
            response = mapping_table.get_item(Key={'FirstServiceLabelID': int(label_id)})
            item = response.get('Item')
            return item.get('SecondServiceTask') if item else None
        except Exception as e:
            logger.error(f"Error querying DynamoDB for task mapping: {str(e)}")
            return None

    def delete_entry(self, entity_id):
        """Deletes a timesheet entry mapping from DynamoDB."""
        try:
            self.table.delete_item(Key={'FirstServiceEntityID': int(entity_id)})
            logger.info(f"DynamoDB entry deleted for Entity ID: {entity_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting DynamoDB entry: {str(e)}")
            return False

    def update_entry(self, entry_data):
        """Updates backup entry data with SQS fallback."""
        try:
            entity_id = entry_data['EntityID']
            logger.info(f"Attempting to update EntityID: {entity_id}")
            
            response = self.table.update_item(
                Key={
                    'FirstServiceEntityID': entity_id
                },
                UpdateExpression="set user_name = :u, project_name = :p, client_name = :c, " 
                                "hours = :h, minutes = :m, note = :n, label_id = :l, "
                                "updated_at = :ua, date_added = :da",
                ExpressionAttributeValues={
                    ':u': entry_data['user_name'],
                    ':p': entry_data['project_name'],
                    ':c': entry_data['client_name'],
                    ':h': entry_data['hours'],
                    ':m': entry_data['minutes'],
                    ':n': entry_data['note'],
                    ':l': entry_data['label_id'],
                    ':ua': entry_data['updated_at'],
                    ':da': entry_data['date_added']
                },
                ReturnValues="UPDATED_NEW"
            )
            
            logger.info(f"Update response: {json.dumps(response, indent=2)}")
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
            
        except Exception as e:
            logger.error(f"DynamoDB update failed: {str(e)}")
            # Use SQS client for fallback
            self.sqs_client.send_message('update_entry', entry_data)
            return False

    # New methods for job operations
    def fetch_jobs(self):
        """Fetches all jobs from DynamoDB."""
        try:
            response = self.table.scan()
            jobs = response['Items']
            
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                jobs.extend(response['Items'])
            
            return jobs
        except Exception as e:
            logger.error(f"Error fetching jobs: {str(e)}")
            raise

    def update_job(self, job):
        """Updates or creates a job entry in DynamoDB."""
        try:
            self.table.put_item(Item=job)
            logger.info(f"Successfully updated job {job['JobID']}")
            return True
        except Exception as e:
            logger.error(f"Failed to update job {job['JobID']}: {str(e)}")
            self.sqs_client.send_message('update_job', job)
            return False

    def delete_job(self, job_id):
        """Deletes a job entry from DynamoDB."""
        try:
            self.table.delete_item(Key={'JobID': job_id})
            logger.info(f"Successfully deleted job {job_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete job {job_id}: {str(e)}")
            self.sqs_client.send_message('delete_job', {'JobID': job_id})
            return False
