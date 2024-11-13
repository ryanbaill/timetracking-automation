from datetime import datetime, UTC
from typing import List, Dict, Optional

"""Constants used throughout the application."""

# Label IDs to exclude from processing
EXCLUDED_LABEL_IDS = [1111, 2222]  # Parent labels to exclude

# Client codes to exclude from synchronization
EXCLUDED_CLIENTS = ['Client1', 'Client2', 'Client3', 'Client4']

# Project defaults
DEFAULT_PROJECT_COLOR = "FFFFFF"
DEFAULT_RATE_TYPE = "project"

# Default user IDs for project access
DEFAULT_USER_IDS = [
    2215558, 2215702, 2232597, 2232598, 2232596, 2215698, 2232599, 2232600,
    2230571, 2215699, 2215700, 2215701, 2244639, 2244640, 2244638, 2244644,
    2244643, 2245192, 2244647, 2244646, 2244641, 2244637
]

# Default label IDs for projects
DEFAULT_LABEL_IDS = range(4018292, 4018306)

# API endpoints
API_ENDPOINTS = {
    'first_service': 'https://api.service1.com/1.1',
    'second_service': 'https://api.service2.com/service/api'
}

# DynamoDB table names
TABLES = {
    'timesheet_entries': 'Timesheet_Entries',
    'task_mappings': 'FirstService_Labels_SecondService_Task_IDs',
    'retention_days': 45  # Added retention days constant
}

# SQS queue names
QUEUES = {
    'timesheet': 'timesheet-queue'
}

# SQS Operation Types
SQS_OPERATIONS = {
    'WRITE': 'write_timesheet_entry',
    'UPDATE': 'update_entry',
    'DELETE': 'delete_entry',
    'BACKUP': 'store_timesheet',
    'BACKUP_UPDATE': 'update_backup',
    'BACKUP_DELETE': 'delete_backup',
    'CREATE_CLIENT': 'create_client',
    'CREATE_PROJECT': 'create_project',
    'UPDATE_JOB': 'update_job',
    'DELETE_JOB': 'delete_job',
    'CLEANUP': 'cleanup_entry'  # Added cleanup operation type
}

# Job sync settings
DEFAULT_TIMEOUT = 30
EXCLUDED_CLIENTS = ['Client1', 'Client2', 'Client3', 'Client4']

# API request defaults
DEFAULT_TIMEOUT = 30
