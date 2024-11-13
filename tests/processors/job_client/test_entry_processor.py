import pytest
import json
import logging
from datetime import datetime, UTC
import traceback
from typing import Dict, List, Optional, Any
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
import boto3
from src.processors.job_client.entry_processor import JobProcessor
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

class TestJobProcessor:
    """Test suite for JobProcessor class."""

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
    def processor(self, mock_apis):
        """Creates a JobProcessor instance with mocked dependencies."""
        first_api, second_api = mock_apis
        return JobProcessor(first_api=first_api, second_api=second_api)

    def test_initialization(self, processor, mock_apis):
        """Tests processor initialization."""
        first_api, second_api = mock_apis
        assert processor.first_api == first_api
        assert processor.second_api == second_api
        assert processor.excluded_clients == EXCLUDED_CLIENTS

    def test_process_clients_success(self, processor):
        """Tests successful client synchronization."""
        processor.first_api.get_clients.return_value = {}
        processor.second_api.get_clients.return_value = {
            'success': True,
            'clients': [
                {'Client Code': 'TEST1', 'Client ID': '123'},
                {'Client Code': 'TEST2', 'Client ID': '456'}
            ]
        }
        processor.first_api.create_client.return_value = {'id': 1}

        result = processor.process_clients()
        assert result['success'] is True
        assert len(result['results']) == 2
        assert result['clients']['test1'] == 1

    def test_process_projects_success(self, processor):
        """Tests successful project synchronization."""
        first_clients = {'test_client': 1}
        test_date = '2024-01-01'
        
        processor.second_api.get_projects.return_value = {
            'success': True,
            'jobs': [
                {
                    'Job Name': 'Test Job',
                    'Job Code': 'J123',
                    'Client Code': 'TEST_CLIENT',
                    'Job ID': 'job123'
                }
            ]
        }
        processor.first_api.get_projects.return_value = {}

        result = processor.process_projects(first_clients, test_date)
        assert result['success'] is True
        assert len(result['results']) == 1
        assert result['results'][0]['created'] is True

    def test_process_clients_api_error(self, processor):
        """Tests error handling when API call fails."""
        processor.second_api.get_clients.return_value = {
            'success': False,
            'error': 'API Error'
        }

        with pytest.raises(Exception) as exc_info:
            processor.process_clients()
        assert "Failed to fetch clients" in str(exc_info.value)

    def test_process_projects_no_client(self, processor):
        """Tests handling of projects with non-existent clients."""
        first_clients = {}
        processor.second_api.get_projects.return_value = {
            'success': True,
            'jobs': [
                {
                    'Job Name': 'Test Job',
                    'Job Code': 'J123',
                    'Client Code': 'NONEXISTENT',
                    'Job ID': 'job123'
                }
            ]
        }
        processor.first_api.get_projects.return_value = {}

        result = processor.process_projects(first_clients)
        assert result['success'] is True
        assert len(result['results']) == 0

