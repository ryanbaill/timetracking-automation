import requests
import logging
import json
from typing import Dict, Optional, Any
from datetime import datetime
from src.utils.constants.constants import DEFAULT_TIMEOUT

logger = logging.getLogger(__name__)

class FirstAPI:
    """Handles all interactions with the first API service."""
    def __init__(self, token, account_id):
        self.token = token
        self.account_id = account_id
        self.base_url = f"https://api.service1.com/1.1/{self.account_id}"
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def get_clients(self):
        """Fetches all clients from first service."""
        url = f"{self.base_url}/clients"
        logger.info(f"Fetching clients from first service. URL: {url}")
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            clients = response.json()
            return {client['name'].strip().lower(): client['id'] for client in clients}
        except Exception as e:
            logger.error(f"Failed to fetch clients: {str(e)}")
            raise

    def create_client(self, client_data):
        """Creates a new client in first service."""
        url = f"{self.base_url}/clients"
        logger.info(f"Creating new client in first service. URL: {url}")
        try:
            response = requests.post(url, headers=self.headers, json={"client": client_data})
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create client: {str(e)}")
            raise

    def get_projects(self):
        """Fetches all projects from first service."""
        url = f"{self.base_url}/projects"
        logger.info(f"Fetching projects from first service. URL: {url}")
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            projects = response.json()
            return {project['external_id']: project for project in projects}
        except Exception as e:
            logger.error(f"Failed to fetch projects: {str(e)}")
            raise

    def create_project(self, project_data):
        """Creates a new project in first service."""
        url = f"{self.base_url}/projects"
        logger.info(f"Creating new project in first service. URL: {url}")
        try:
            response = requests.post(url, headers=self.headers, json={"project": project_data})
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create project: {str(e)}")
            raise

    def update_project(self, project_id, project_data):
        """Updates an existing project in first service."""
        url = f"{self.base_url}/projects/{project_id}"
        logger.info(f"Updating project {project_id} in first service")
        try:
            response = requests.put(url, headers=self.headers, json={"project": project_data})
            response.raise_for_status()
            logger.info(f"Successfully updated project {project_data.get('name', project_id)}")
            return True
        except Exception as e:
            logger.error(f"Failed to update project {project_id}: {str(e)}")
            return False

    def delete_project(self, project_id):
        """Deletes a project from first service."""
        url = f"{self.base_url}/projects/{project_id}"
        logger.info(f"Deleting project {project_id} from first service")
        try:
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()
            logger.info(f"Successfully deleted project {project_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete project {project_id}: {str(e)}")
            return False
