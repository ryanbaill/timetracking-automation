import requests
import logging
import json
from datetime import datetime, UTC
from typing import Dict, Optional, Any, List
from src.utils.constants.constants import (
    EXCLUDED_CLIENTS,
    DEFAULT_TIMEOUT
)

logger = logging.getLogger(__name__)

class SecondAPI:
    """Handles all interactions with the second API service."""
    def __init__(self, org_code, username, password, user_id):
        self.org_code = org_code
        self.username = username
        self.password = password
        self.user_id = user_id
        self.app_id = None
        self.session = requests.Session()
        self.excluded_clients = ['Client1', 'Client2', 'Client3', 'Client4']

    def authenticate(self):
        """Authenticates with the API service and returns app_id."""
        auth_url = "https://api.service2.com/service/api/login/"
        auth_data = {
            "cmd": "org",
            "idOrg": self.org_code,
            "strUsername": self.username,
            "strPassword": self.password
        }
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        logger.info(f"Attempting to authenticate with second service. URL: {auth_url}")
        try:
            auth_response = self.session.post(auth_url, data=auth_data, headers=headers, timeout=30)
            auth_response.raise_for_status()
            auth_data = auth_response.json()
            if 'appID' not in auth_data:
                raise Exception("Authentication failed: appID not found in response")
            self.app_id = auth_data['appID']
            logger.info("Second service authentication successful")
            return self.app_id
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception("Authentication failed", str(e))

    def get_clients(self):
        """Fetches all active clients from second service."""
        if not self.app_id:
            raise ValueError("Must authenticate first")
        
        url = f"https://api.service2.com/service/api/client/?o={self.org_code}&i={self.user_id}&cmd=list&boolArchived=0"
        headers = {'Cookie': f'appID={self.app_id}; appOrganization={self.org_code}; appUsername={self.username}'}
        
        try:
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'listClients' in data and 'data' in data['listClients']:
                clients = []
                for client_data in data['listClients']['data']:
                    client_code = client_data[1]
                    if client_code not in self.excluded_clients:
                        clients.append({
                            'Client ID': client_data[0],
                            'Client Code': client_code,
                            'Client Name': client_data[2]
                        })
                return {'success': True, 'clients': clients}
            return {'success': False, 'error': 'No client data found'}
        except Exception as e:
            logger.error(f"Failed to fetch clients: {str(e)}")
            return {'success': False, 'error': str(e)}

    def get_projects(self, date=None):
        """Fetches projects from second service."""
        if not self.app_id:
            raise ValueError("Must authenticate first")
        
        if date is None:
            date = datetime.now(UTC).strftime("%Y-%m-%d")
            
        url = f"https://api.service2.com/service/api/reports/?o={self.org_code}&i={self.user_id}&cmd=run&gidReport=JobListCustomizable&boolSaveState=0&idRangeJobCreatedDate=10&dtFromJobCreatedDate={date}&dtToJobCreatedDate={date}"
        headers = {'Cookie': f'appID={self.app_id}; appOrganization={self.org_code}; appUsername={self.username}'}
        
        try:
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            jobs = []
            for result in data.get("results", []):
                if isinstance(result, dict) and "hdr" in result and "data" in result:
                    for row in result["data"]:
                        jobs.append({
                            'Job ID': row[result["hdr"]['Job ID']],
                            'Job Code': row[result["hdr"]['Job Code']],
                            'Job Name': row[result["hdr"]['Job Name']],
                            'Client ID': row[result["hdr"]['Client ID']],
                            'Client Code': row[result["hdr"]['Client Code']]
                        })
            return {'success': True, 'jobs': jobs}
        except Exception as e:
            logger.error(f"Failed to fetch projects: {str(e)}")
            return {'success': False, 'error': str(e)}

    def fetch_all_jobs(self):
        """Fetches all active jobs from the API."""
        if not self.app_id:
            logger.error("No app_id found. Must authenticate first")
            return []

        url = f"https://api.service2.com/service/api/job/?o={self.org_code}&i={self.user_id}&cmd=list&boolArchived=0&boolClosed=0"
        headers = {'Cookie': f'appID={self.app_id}; appOrganization={self.org_code}; appUsername={self.username}'}
        
        try:
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            response_data = response.json()
            
            jobs = []
            list_jobs = response_data.get("listJobs", {})
            hdr = list_jobs.get("hdr", {})
            data = list_jobs.get("data", [])
            
            for row in data:
                client_code = row[hdr["strClientCode"]]
                if client_code not in self.excluded_clients:
                    job = {
                        'JobID': int(row[hdr["idJob"]]),
                        'ClientID': int(row[hdr["idClient"]]),
                        'ClientCode': client_code,
                        'ClientName': row[hdr["strClientName"]],
                        'JobCode': row[hdr["strJobCode"]],
                        'JobName': row[hdr["strJobName"]]
                    }
                    jobs.append(job)
            
            logger.info(f"Successfully fetched {len(jobs)} active jobs")
            return jobs
        except requests.Timeout:
            logger.error("Request timed out while fetching jobs")
            return []
        except requests.RequestException as e:
            logger.error(f"Failed to fetch jobs: {str(e)}")
            return []
