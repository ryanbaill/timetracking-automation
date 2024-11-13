import logging
from datetime import datetime, UTC
import traceback
from typing import Dict, List, Optional, Any
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import (
    EXCLUDED_CLIENTS,
    DEFAULT_PROJECT_COLOR,
    DEFAULT_RATE_TYPE,
    DEFAULT_USER_IDS,
    DEFAULT_LABEL_IDS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS
)

# Constants
EXCLUDED_CLIENTS = ['Client1', 'Client2', 'Client3', 'Client4']
DEFAULT_PROJECT_COLOR = "FFFFFF"
DEFAULT_RATE_TYPE = "project"

logger = logging.getLogger(__name__)

class JobProcessor:
    """Processes client and job synchronization between services."""
    def __init__(self, first_api, second_api):
        self.first_api = first_api
        self.second_api = second_api
        self.excluded_clients = EXCLUDED_CLIENTS

    def process_clients(self):
        """Synchronizes clients from second service to first service."""
        try:
            first_clients = self.first_api.get_clients()
            second_clients = self.second_api.get_clients()
            
            if not second_clients['success']:
                raise Exception(f"Failed to fetch clients: {second_clients.get('error')}")

            results = []
            for client in second_clients['clients']:
                client_code = client['Client Code'].strip().lower()
                if client_code not in first_clients:
                    client_data = {
                        "name": client['Client Code'],
                        "active": True,
                        "external_id": client['Client ID']
                    }
                    try:
                        new_client = self.first_api.create_client(client_data)
                        first_clients[client_code] = new_client['id']
                        results.append({'created': True, 'client_code': client['Client Code']})
                    except Exception as e:
                        results.append({
                            'created': False, 
                            'client_code': client['Client Code'], 
                            'error': str(e),
                            'retry_data': client_data
                        })
                else:
                    results.append({'created': False, 'client_code': client['Client Code'], 'exists': True})

            return {'success': True, 'results': results, 'clients': first_clients}
            
        except Exception as e:
            error_msg = f"Client synchronization failed: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def process_projects(self, first_clients, date=None):
        """Synchronizes projects from second service to first service."""
        try:
            if date is None:
                date = datetime.now(UTC).strftime("%Y-%m-%d")

            second_projects = self.second_api.get_projects(date)
            if not second_projects['success']:
                raise Exception(f"Failed to fetch projects: {second_projects.get('error')}")

            first_projects = self.first_api.get_projects()
            results = []

            for job in second_projects['jobs']:
                job_name = f"{job['Job Name']} - {job['Job Code']}".strip().lower()
                if job_name not in first_projects:
                    client_code = job['Client Code'].strip().lower()
                    if client_code in first_clients:
                        project_data = {
                            "name": f"{job['Job Name']} - {job['Job Code']}",
                            "client_id": first_clients[client_code],
                            "color": DEFAULT_PROJECT_COLOR,
                            "rate_type": DEFAULT_RATE_TYPE,
                            "users": [{"user_id": id} for id in [2215558, 2215702, 2232597, 2232598, 2232596, 2215698, 2232599, 2232600, 2230571, 2215699, 2215700, 2215701, 2244639, 2244640, 2244638, 2244644, 2244643, 2245192, 2244647, 2244646, 2244641, 2244637]],
                            "labels": [{"required": True if i == 0 else False, "label_id": id} for i, id in enumerate(range(4018292, 4018306))],
                            "enable_labels": "custom",
                            "external_id": job['Job ID']
                        }
                        try:
                            self.first_api.create_project(project_data)
                            results.append({'created': True, 'project_name': job['Job Name']})
                        except Exception as e:
                            results.append({
                                'created': False, 
                                'project_name': job['Job Name'], 
                                'error': str(e),
                                'retry_data': project_data
                            })
                    else:
                        logger.warning(f"No client found for project {job['Job Name']}")
                else:
                    results.append({'created': False, 'project_name': job['Job Name'], 'exists': True})

            return {'success': True, 'results': results}
            
        except Exception as e:
            error_msg = f"Project synchronization failed: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise Exception(error_msg)
