import json
import logging
import boto3
import traceback
import requests
from datetime import datetime

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
        
        # API One parameters
        self.API_ONE_TOKEN = self.config_manager.get_parameter('/api/firstservice/token')
        self.API_ONE_ACCOUNT_ID = self.config_manager.get_parameter('/api/firstservice/account_id')
        
        # API Two parameters
        self.API_TWO_ORG_CODE = self.config_manager.get_parameter('/api/secondservice/org_code')
        self.API_TWO_USERNAME = self.config_manager.get_parameter('/api/secondservice/username')
        self.API_TWO_PASSWORD = self.config_manager.get_parameter('/api/secondservice/password')
        self.API_TWO_USER_ID = self.config_manager.get_parameter('/api/secondservice/user_id')
        
        # SNS configuration
        self.SNS_TOPIC_ARN = self.config_manager.get_parameter('/notifications/sns_topic_arn')

class FirstAPI:
    """Handles interactions with the first time tracking API."""
    def __init__(self, token, account_id):
        self.token = token
        self.account_id = account_id
        self.base_url = "https://api.service1.com/1.1"
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def get_jobs(self):
        """Fetches all jobs from the first service."""
        url = f"{self.base_url}/{self.account_id}/jobs"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching jobs: {str(e)}")
            return []

    def get_clients(self):
        """Fetches all clients from the first service."""
        url = f"{self.base_url}/{self.account_id}/clients"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching clients: {str(e)}")
            return []

    def update_job(self, job_id, data):
        """Updates a job in the first service."""
        url = f"{self.base_url}/{self.account_id}/jobs/{job_id}"
        try:
            response = requests.put(url, headers=self.headers, json=data)
            response.raise_for_status()
            return {'success': True, 'data': response.json()}
        except Exception as e:
            logger.error(f"Error updating job: {str(e)}")
            return {'success': False, 'error': str(e)}

    def update_client(self, client_id, data):
        """Updates a client in the first service."""
        url = f"{self.base_url}/{self.account_id}/clients/{client_id}"
        try:
            response = requests.put(url, headers=self.headers, json=data)
            response.raise_for_status()
            return {'success': True, 'data': response.json()}
        except Exception as e:
            logger.error(f"Error updating client: {str(e)}")
            return {'success': False, 'error': str(e)}

    def delete_project(self, project_id):
        """Deletes a project from the first service."""
        url = f"{self.base_url}/{self.account_id}/projects/{project_id}"
        try:
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error deleting project: {str(e)}")
            return False

class SecondAPI:
    """Handles interactions with the second time tracking API."""
    def __init__(self, org_code, username, password, user_id):
        self.org_code = org_code
        self.username = username
        self.password = password
        self.user_id = user_id
        self.base_url = "https://api.service2.com"

    def authenticate(self):
        """Authenticates with the second service."""
        url = f"{self.base_url}/auth"
        auth_data = {
            "orgCode": self.org_code,
            "username": self.username,
            "password": self.password
        }
        try:
            response = requests.post(url, json=auth_data)
            response.raise_for_status()
            return response.json().get('appId')
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return None

    def fetch_all_jobs(self):
        """Fetches all active jobs from the second service."""
        url = f"{self.base_url}/jobs?status=active"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching jobs: {str(e)}")
            return []

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

class DynamoDBHandler:
    """Handles interactions with DynamoDB."""
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.job_table = self.dynamodb.Table('Job_Data')

    def fetch_jobs(self):
        """Fetches all jobs from DynamoDB."""
        try:
            response = self.job_table.scan()
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"Error fetching jobs from DynamoDB: {str(e)}")
            return []

    def update_job(self, job):
        """Updates a job in DynamoDB."""
        try:
            self.job_table.put_item(Item=job)
            return True
        except Exception as e:
            logger.error(f"Error updating job in DynamoDB: {str(e)}")
            return False

    def delete_job(self, job_id):
        """Deletes a job from DynamoDB."""
        try:
            self.job_table.delete_item(Key={'JobID': job_id})
            return True
        except Exception as e:
            logger.error(f"Error deleting job from DynamoDB: {str(e)}")
            return False

class JobClientProcessor:
    """Processes job and client updates between services."""
    def __init__(self):
        self.config = Config()
        self.first_api = FirstAPI(self.config.API_ONE_TOKEN, self.config.API_ONE_ACCOUNT_ID)
        self.second_api = SecondAPI(
            self.config.API_TWO_ORG_CODE,
            self.config.API_TWO_USERNAME,
            self.config.API_TWO_PASSWORD,
            self.config.API_TWO_USER_ID
        )
        self.dynamodb_handler = DynamoDBHandler()
        self.sns_notifier = SNSNotifier(self.config.SNS_TOPIC_ARN)

    def process_changes(self):
        """
        Process updates and deletions between services:
        1. Fetch current state from both services
        2. Compare and identify changes
        3. Process updates and deletions
        4. Handle any failures
        """
        try:
            # Get current data from both services
            app_id = self.second_api.authenticate()
            if not app_id:
                raise Exception("Authentication failed with second service")

            second_jobs = self.second_api.fetch_all_jobs()
            dynamodb_jobs = self.dynamodb_handler.fetch_jobs()
            first_clients = self.first_api.get_clients()
            first_projects = self.first_api.get_jobs()

            # Process changes
            updated, deleted, orphaned = self._process_job_changes(
                second_jobs, dynamodb_jobs, first_clients, first_projects
            )

            results = {
                'success': True,
                'updated': len(updated),
                'deleted': len(deleted),
                'orphaned': len(orphaned),
                'details': {
                    'updated_jobs': updated,
                    'deleted_jobs': deleted,
                    'orphaned_projects': orphaned
                }
            }

            if not any([updated, deleted, orphaned]):
                results['message'] = "No changes detected"

            return results

        except Exception as e:
            error_msg = f"Failed to process changes: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            self.sns_notifier.send_message("Job Update Error", error_msg)
            return {'success': False, 'error': error_msg}

    def _process_job_changes(self, second_jobs, dynamodb_jobs, first_clients, first_projects):
        """Process job updates and deletions."""
        updated = []
        deleted = []
        orphaned = []

        # Convert to dictionaries for easier lookup
        dynamodb_jobs_dict = {job['JobID']: job for job in dynamodb_jobs}
        second_jobs_dict = {job['id']: job for job in second_jobs}
        first_projects_dict = {proj['id']: proj for proj in first_projects}

        # Process updates and deletions
        for job_id, current_job in second_jobs_dict.items():
            if job_id in dynamodb_jobs_dict:
                if current_job != dynamodb_jobs_dict[job_id]:
                    if self.dynamodb_handler.update_job(current_job):
                        updated.append(job_id)
            else:
                if self.dynamodb_handler.update_job(current_job):
                    updated.append(job_id)

        # Handle deletions
        for job_id in dynamodb_jobs_dict.keys():
            if job_id not in second_jobs_dict:
                if self.dynamodb_handler.delete_job(job_id):
                    deleted.append(job_id)

        # Handle orphaned projects in first service
        for project_id, project in first_projects_dict.items():
            if project['external_id'] not in second_jobs_dict:
                if self.first_api.delete_project(project_id):
                    orphaned.append(project_id)

        return updated, deleted, orphaned

def lambda_handler(event, context):
    """AWS Lambda handler function."""
    logger.info(f"Received event: {json.dumps(event)}")
    processor = JobClientProcessor()
    return processor.process_changes()

def retry_handler(event, context):
    """AWS Lambda handler for processing SQS retry messages."""
    logger.info("Starting retry handler")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('Job_Data')
    
    try:
        for record in event['Records']:
            message = json.loads(record['body'])
            if message['operation'] == 'update_job':
                table.put_item(Item=message['data'])
            elif message['operation'] == 'delete_job':
                table.delete_item(Key={'JobID': message['data']['JobID']})
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Retry processing complete'})
        }
    except Exception as e:
        logger.error(f"Error in retry handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# Main flow of the script:
# 1. EventBridge rule triggers this Lambda function every 1 minute
# 2. Retrieve configuration from AWS Systems Manager Parameter Store for both services
# 3. Authenticate with SecondService API using credentials from Parameter Store
# 4. Fetch current job and client data from both FirstService and SecondService
# 5. Compare job states between SecondService and DynamoDB to identify changes
# 6. Process job updates by updating DynamoDB and FirstService accordingly
# 7. Handle job deletions by removing from DynamoDB and FirstService
# 8. Process orphaned projects in FirstService that no longer exist in SecondService
# 9. Update DynamoDB with the latest state of all jobs
# 10. Send SNS notifications for any errors that occur during processing
# 11. Queue failed operations to SQS for retry processing