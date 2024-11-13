import logging
import traceback
from datetime import datetime, UTC
from typing import Dict, List, Optional, Any, Tuple
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import (
    EXCLUDED_CLIENTS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS,
    TABLES
)

logger = logging.getLogger(__name__)

class JobUpdateProcessor:
    """Processes client and job updates/deletions between services."""
    def __init__(self, first_api, second_api, dynamodb_handler, sns_notifier, sqs_client):
        self.first_api = first_api
        self.second_api = second_api
        self.dynamodb_handler = dynamodb_handler
        self.sns_notifier = sns_notifier
        self.sqs_client = sqs_client

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
            self.second_api.authenticate()
            second_jobs = self.second_api.fetch_all_jobs()
            if not second_jobs:
                logger.warning("No jobs found in second service")
                return {'success': False, 'error': 'No jobs found in second service'}

            dynamodb_jobs = self.dynamodb_handler.fetch_jobs()
            first_clients = self.first_api.get_clients()
            if not first_clients:
                logger.warning("No clients found in first service")
                return {'success': False, 'error': 'No clients found in first service'}

            first_projects = self.first_api.get_projects()
            
            # Process changes
            updated, deleted, orphaned = self.process_job_changes(
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
                logger.info("No changes detected")
                results['message'] = "No changes detected"

            return results

        except Exception as e:
            error_msg = f"Failed to process changes: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def process_job_changes(self, second_jobs, dynamodb_jobs, first_clients, first_projects):
        """Process job updates and deletions."""
        updated = []
        deleted = []
        orphaned = []

        # Convert to dictionaries for easier lookup
        dynamodb_jobs_dict = {job['JobID']: job for job in dynamodb_jobs}
        second_jobs_dict = {job['JobID']: job for job in second_jobs}

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
        for project_id, project in first_projects.items():
            try:
                if int(project['external_id']) not in second_jobs_dict:
                    if self.first_api.delete_project(project['id']):
                        orphaned.append(project_id)
            except (ValueError, KeyError) as e:
                logger.warning(f"Error processing project {project_id}: {str(e)}")

        return updated, deleted, orphaned

    def handle_error(self, error_message):
        logger.error(error_message)
        self.sns_notifier.send_message("Job Update Error", error_message)
        return {
            'success': False,
            'error': error_message
        }
