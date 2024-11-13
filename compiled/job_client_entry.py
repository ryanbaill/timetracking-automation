import json
import logging
import requests
import boto3
import traceback
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
        self.SQS_QUEUE_URL = self.config_manager.get_parameter('/sqs/queue_url')

class FirstAPI:
    """Handles interactions with the first service API."""
    def __init__(self, token, account_id):
        self.token = token
        self.account_id = account_id
        self.base_url = "https://api.service1.com/1.1"
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

    def get_clients(self):
        """Fetches all clients from first service."""
        url = f"{self.base_url}/{self.account_id}/clients"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching clients: {str(e)}")
            return None

    def get_projects(self):
        """Fetches all projects from first service."""
        url = f"{self.base_url}/{self.account_id}/projects"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching projects: {str(e)}")
            return None

    def create_client(self, client_data):
        """Creates a new client in first service."""
        url = f"{self.base_url}/{self.account_id}/clients"
        try:
            response = requests.post(url, headers=self.headers, json=client_data)
            response.raise_for_status()
            return {'success': True, 'data': response.json()}
        except Exception as e:
            logger.error(f"Error creating client: {str(e)}")
            return {'success': False, 'error': str(e)}

    def create_project(self, project_data):
        """Creates a new project in first service."""
        url = f"{self.base_url}/{self.account_id}/projects"
        try:
            response = requests.post(url, headers=self.headers, json=project_data)
            response.raise_for_status()
            return {'success': True, 'data': response.json()}
        except Exception as e:
            logger.error(f"Error creating project: {str(e)}")
            return {'success': False, 'error': str(e)}

class SecondAPI:
    """Handles interactions with the second service API."""
    def __init__(self, org_code, username, password, user_id):
        self.org_code = org_code
        self.username = username
        self.password = password
        self.user_id = user_id
        self.base_url = "https://api.service2.com"
        self.app_id = None

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
            self.app_id = response.json().get('appId')
            return self.app_id
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return None

    def fetch_all_jobs(self):
        """Fetches all jobs from second service."""
        if not self.app_id:
            self.authenticate()
        
        url = f"{self.base_url}/jobs"
        headers = {"AppId": self.app_id}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching jobs: {str(e)}")
            return None

class SNSNotifier:
    """Handles sending notifications through AWS SNS."""
    def __init__(self, topic_arn):
        self.sns = boto3.client('sns')
        self.topic_arn = topic_arn

    def send_message(self, subject, message):
        try:
            self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=subject,
                Message=message
            )
            logger.info(f"SNS notification sent: {subject}")
        except Exception as e:
            logger.error(f"Failed to send SNS notification: {str(e)}")

class SQSClient:
    """Handles interactions with AWS SQS."""
    def __init__(self, queue_url):
        self.sqs = boto3.client('sqs')
        self.queue_url = queue_url

    def send_message(self, operation, data):
        """Sends a message to SQS for retry processing."""
        try:
            message = {
                'operation': operation,
                'data': data
            }
            self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(message)
            )
            logger.info(f"Message sent to SQS: {operation}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to SQS: {str(e)}")
            return False

class JobProcessor:
    """Processes job and client synchronization between services."""
    def __init__(self, first_api, second_api):
        self.first_api = first_api
        self.second_api = second_api

    def process_clients(self):
        """Process client synchronization."""
        try:
            second_jobs = self.second_api.fetch_all_jobs()
            if not second_jobs:
                return {'success': False, 'error': 'No jobs found in second service'}

            first_clients = self.first_api.get_clients()
            results = []
            
            for job in second_jobs:
                if not self._client_exists(job['client'], first_clients):
                    client_result = self.first_api.create_client({
                        'name': job['client'],
                        'external_id': job['client_id']
                    })
                    results.append({
                        'client_id': job['client_id'],
                        'created': client_result['success'],
                        'retry_data': {
                            'name': job['client'],
                            'external_id': job['client_id']
                        } if not client_result['success'] else None
                    })

            return {
                'success': True,
                'clients': second_jobs,
                'results': results
            }
        except Exception as e:
            logger.error(f"Error processing clients: {str(e)}")
            return {'success': False, 'error': str(e)}

    def process_projects(self, second_jobs):
        """Process project synchronization."""
        try:
            first_projects = self.first_api.get_projects()
            results = []
            
            for job in second_jobs:
                if not self._project_exists(job['id'], first_projects):
                    project_result = self.first_api.create_project({
                        'name': job['name'],
                        'external_id': job['id'],
                        'client_id': job['client_id']
                    })
                    results.append({
                        'job_id': job['id'],
                        'created': project_result['success'],
                        'retry_data': {
                            'name': job['name'],
                            'external_id': job['id'],
                            'client_id': job['client_id']
                        } if not project_result['success'] else None
                    })

            return {
                'success': True,
                'results': results
            }
        except Exception as e:
            logger.error(f"Error processing projects: {str(e)}")
            return {'success': False, 'error': str(e)}

    def _client_exists(self, client_name, first_clients):
        """Check if client exists in first service."""
        return any(client['name'] == client_name for client in first_clients)

    def _project_exists(self, external_id, first_projects):
        """Check if project exists in first service."""
        return any(str(project['external_id']) == str(external_id) for project in first_projects)

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
                'message': 'Synchronization complete',
                'client_results': client_sync['results'],
                'project_results': project_sync['results']
            })
        }

    except Exception as e:
        error_message = f"Error in synchronization: {str(e)}"
        logger.error(f"{error_message}\n{traceback.format_exc()}")
        sns_notifier.send_message("Synchronization Error", error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

def retry_handler(event, context):
    """AWS Lambda handler for processing SQS retry messages."""
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
            'body': json.dumps({'message': 'Retry processing complete'})
        }
    except Exception as e:
        logger.error(f"Error in retry handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# Main flow of the script:
# 1. EventBridge triggers this Lambda function every 1 minute to sync new clients and projects from SecondService to FirstService
# 2. Retrieve configuration from AWS Systems Manager Parameter Store
# 3. Initialize API clients for both services
# 4. Initialize notification services (SNS and SQS)
# 5. Authenticate with SecondService
# 6. Process client synchronization:
#    - Fetch clients from SecondService
#    - Compare with FirstService clients
#    - Create missing clients in FirstService
# 7. Process project synchronization:
#    - Fetch projects for synced clients
#    - Create missing projects in FirstService
# 8. Handle any failed operations by sending to SQS for retry
# 9. Send notifications for any errors via SNS