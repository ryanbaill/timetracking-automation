import pytest
import json
import logging
import traceback
from datetime import datetime, UTC
from typing import Dict, List, Optional, Any, Tuple
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
import boto3
from src.processors.job_client.update_processor import JobUpdateProcessor
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


class TestJobUpdateProcessor:
    """Test suite for JobUpdateProcessor class."""

    @pytest.fixture
    def mock_apis(self):
        """Creates mock API instances."""
        with patch('src.utils.apis.first_api.FirstAPI') as mock_first_api, \
             patch('src.utils.apis.second_api.SecondAPI') as mock_second_api:
            
            first_api_instance = MagicMock()
            second_api_instance = MagicMock()
            
            mock_first_api.return_value = first_api_instance
            mock_second_api.return_value = second_api_instance
            
            yield first_api_instance, second_api_instance

    @pytest.fixture
    def mock_dynamodb(self):
        """Creates mock DynamoDB handler."""
        with patch('src.utils.aws.dynamodb.DynamoDBHandler') as mock:
            mock_instance = MagicMock()
            yield mock_instance

    @pytest.fixture
    def mock_sns(self):
        """Creates mock SNS notifier."""
        with patch('src.utils.aws.sns.SNSNotifier') as mock:
            mock_instance = MagicMock()
            yield mock_instance

    @pytest.fixture
    def mock_sqs(self):
        """Creates mock SQS client."""
        with patch('src.utils.aws.sqs.SQSClient') as mock:
            mock_instance = MagicMock()
            yield mock_instance

    @pytest.fixture
    def processor(self, mock_apis, mock_dynamodb, mock_sns, mock_sqs):
        """Creates a JobUpdateProcessor instance with mocked dependencies."""
        first_api, second_api = mock_apis
        return JobUpdateProcessor(
            first_api=first_api,
            second_api=second_api,
            dynamodb_handler=mock_dynamodb,
            sns_notifier=mock_sns,
            sqs_client=mock_sqs
        )

    def test_initialization(self, processor, mock_apis):
        """Tests processor initialization."""
        first_api, second_api = mock_apis
        assert processor.first_api == first_api
        assert processor.second_api == second_api
        assert processor.dynamodb_handler is not None
        assert processor.sns_notifier is not None
        assert processor.sqs_client is not None

    def test_process_changes_success(self, processor):
        """Tests successful processing of changes."""
        # Mock data
        second_jobs = [
            {'JobID': 1, 'Name': 'Job 1', 'Status': 'Active'},
            {'JobID': 2, 'Name': 'Job 2', 'Status': 'Active'}
        ]
        dynamodb_jobs = [
            {'JobID': 1, 'Name': 'Job 1', 'Status': 'Inactive'},
            {'JobID': 3, 'Name': 'Job 3', 'Status': 'Active'}
        ]
        first_clients = {'Client 1': 1, 'Client 2': 2}
        first_projects = {
            'proj1': {'id': 1, 'external_id': '1'},
            'proj2': {'id': 2, 'external_id': '3'}
        }

        # Configure mocks
        processor.second_api.fetch_all_jobs.return_value = second_jobs
        processor.dynamodb_handler.fetch_jobs.return_value = dynamodb_jobs
        processor.first_api.get_clients.return_value = first_clients
        processor.first_api.get_projects.return_value = first_projects
        processor.dynamodb_handler.update_job.return_value = True
        processor.dynamodb_handler.delete_job.return_value = True
        processor.first_api.delete_project.return_value = True

        # Execute test
        result = processor.process_changes()

        # Verify results
        assert result['success'] is True
        assert result['updated'] == 2  # Job 1 updated, Job 2 added
        assert result['deleted'] == 1  # Job 3 deleted
        assert result['orphaned'] == 1  # Project with external_id 3 orphaned

    def test_process_changes_no_second_jobs(self, processor):
        """Tests handling when no jobs found in second service."""
        processor.second_api.fetch_all_jobs.return_value = []
        
        result = processor.process_changes()
        assert result['success'] is False
        assert 'No jobs found in second service' in result['error']

    def test_process_changes_no_first_clients(self, processor):
        """Tests handling when no clients found in first service."""
        processor.second_api.fetch_all_jobs.return_value = [{'JobID': 1}]
        processor.first_api.get_clients.return_value = {}
        
        result = processor.process_changes()
        assert result['success'] is False
        assert 'No clients found in first service' in result['error']

    def test_process_job_changes(self, processor):
        """Tests job changes processing logic."""
        second_jobs = [{'JobID': 1, 'Name': 'Updated Job'}]
        dynamodb_jobs = [{'JobID': 1, 'Name': 'Old Job'}, {'JobID': 2, 'Name': 'Deleted Job'}]
        first_clients = {'Client 1': 1}
        first_projects = {'proj1': {'id': 1, 'external_id': '2'}}

        processor.dynamodb_handler.update_job.return_value = True
        processor.dynamodb_handler.delete_job.return_value = True
        processor.first_api.delete_project.return_value = True

        updated, deleted, orphaned = processor.process_job_changes(
            second_jobs, dynamodb_jobs, first_clients, first_projects
        )

        assert len(updated) == 1
        assert len(deleted) == 1
        assert len(orphaned) == 1
        assert 1 in updated
        assert 2 in deleted
        assert 'proj1' in orphaned
