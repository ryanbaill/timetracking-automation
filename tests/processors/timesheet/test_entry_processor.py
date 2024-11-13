import pytest
import json
import logging
import traceback
import boto3
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Any
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from src.processors.timesheet.entry_processor import TimeEntryProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import (
    EXCLUDED_LABEL_IDS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS
)

class TestTimeEntryProcessor:
    """Test suite for TimeEntryProcessor class."""

    @pytest.fixture
    def mock_first_api(self):
        """Creates mock First API."""
        with patch('src.processors.timesheet.entry_processor.FirstAPI') as mock:
            mock_instance = MagicMock()
            mock_instance.fetch_events.return_value = {
                'id': '12345',
                'label_ids': [4444],
                'project': {'external_id': 'proj123'}
            }
            mock_instance.fetch_user.return_value = {'external_id': 'user123'}
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_second_api(self):
        """Creates mock Second API."""
        with patch('src.processors.timesheet.entry_processor.SecondAPI') as mock:
            mock_instance = MagicMock()
            mock_instance.authenticate.return_value = "session_id"
            mock_instance.submit_timesheet.return_value = {
                'success': True,
                'idTimesheet': 'ts789'
            }
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_config(self):
        """Creates a mock configuration."""
        with patch('src.processors.timesheet.entry_processor.Config') as mock:
            mock_instance = MagicMock()
            mock_instance.API_ONE_TOKEN = 'mock_api_one_token'
            mock_instance.API_ONE_ACCOUNT_ID = 'mock_account_id'
            mock_instance.API_TWO_ORG_CODE = 'mock_org_code'
            mock_instance.API_TWO_USERNAME = 'mock_username'
            mock_instance.API_TWO_PASSWORD = 'mock_password'
            mock_instance.API_TWO_USER_ID = 'mock_user_id'
            mock_instance.DYNAMODB_TABLE = 'test-table'
            mock_instance.SNS_TOPIC_ARN = 'test:arn'
            mock_instance.AWS_REGION = 'us-east-1'
            mock.return_value = mock_instance
            yield mock

    @pytest.fixture
    def mock_dynamodb(self):
        """Creates mock DynamoDB handler."""
        with patch('src.processors.timesheet.entry_processor.DynamoDBHandler') as mock:
            mock_instance = MagicMock()
            mock_instance.get_second_task.return_value = 'task456'
            mock_instance.write_timesheet_entry.return_value = True
            mock.return_value = mock_instance
            yield mock

    @pytest.fixture
    def mock_sns(self):
        """Creates mock SNS notifier."""
        with patch('src.processors.timesheet.entry_processor.SNSNotifier', autospec=True) as mock:
            yield mock

    @pytest.fixture
    def processor(self, mock_config, mock_dynamodb, mock_sns, mock_first_api, mock_second_api):
        """Creates a TimeEntryProcessor instance with mocked dependencies."""
        with patch('src.processors.timesheet.entry_processor.Config', mock_config), \
             patch('src.processors.timesheet.entry_processor.DynamoDBHandler', mock_dynamodb), \
             patch('src.processors.timesheet.entry_processor.SNSNotifier', mock_sns), \
             patch('src.processors.timesheet.entry_processor.FirstAPI', mock_first_api), \
             patch('src.processors.timesheet.entry_processor.SecondAPI', mock_second_api), \
             patch('src.processors.timesheet.entry_processor.boto3.client') as mock_sqs:
            mock_sqs.return_value.get_queue_url.return_value = {'QueueUrl': 'test:url'}
            return TimeEntryProcessor()

    def test_initialization(self, processor):
        """Tests processor initialization."""
        assert processor.first_api is not None
        assert processor.second_api is not None
        assert processor.dynamodb_handler is not None
        assert processor.sns_notifier is not None
        assert processor.sqs is not None
        assert processor.queue_url is not None

    def test_process_event_invalid_payload(self, processor):
        """Tests processing with invalid payload."""
        invalid_webhook = {"invalid": "data"}
        response = processor.process_event(invalid_webhook)
        assert response['statusCode'] == 400
        assert "Invalid Event" in response['body']

    def test_process_event_ai_suggestion(self, processor):
        """Tests handling of AI-generated suggestions."""
        ai_webhook = {
            "payload": {
                "entity_id": "123",
                "entity_path": "suggested_hours/123"
            }
        }
        response = processor.process_event(ai_webhook)
        assert response['statusCode'] == 200
        assert "Skipped Entry" in response['body']

    def test_process_event_no_valid_labels(self, processor, mock_first_api):
        """Tests handling when no valid label IDs are found."""
        webhook_data = {
            "payload": {
                "entity_id": "123",
                "entity_path": "timesheets/123"
            }
        }
        
        mock_first_api.fetch_events.return_value = {
            'label_ids': EXCLUDED_LABEL_IDS  # Only excluded labels
        }
        
        response = processor.process_event(webhook_data)
        assert response['statusCode'] == 200
        assert "Invalid Entry" in response['body']

    def test_process_event_success(self, processor):
        """Tests successful event processing."""
        webhook_data = {
            "payload": {
                "entity_id": "123",
                "entity_path": "timesheets/123"
            }
        }

        with patch.object(processor.first_api, 'fetch_events') as mock_fetch, \
             patch.object(processor.first_api, 'fetch_user') as mock_user, \
             patch.object(processor.second_api, 'authenticate') as mock_auth, \
             patch.object(processor.second_api, 'fetch_tasks') as mock_tasks, \
             patch.object(processor.second_api, 'submit_timesheet') as mock_submit, \
             patch.object(processor.dynamodb_handler, 'get_second_task') as mock_get_task, \
             patch.object(processor.dynamodb_handler, 'write_timesheet_entry') as mock_write:

            # Add the required methods to the processor class
            processor.find_matching_task_id = lambda tasks, task_name: 'task456'
            processor.parse_firstservice_event = lambda event: {
                'user': {'id': 'user123'},
                'additional_info': {'day': '2024-03-20'}
            }

            # Mock first service responses
            mock_fetch.return_value = {
                'id': '12345',
                'label_ids': [4444],
                'project': {'external_id': 'proj123'},
                'user': {'id': 'user123'},
                'duration': 3600,
                'additional_info': {'day': '2024-03-20'}
            }
            mock_user.return_value = {'external_id': 'user123'}

            # Mock second service responses
            mock_auth.return_value = "session_id"
            mock_tasks.return_value = [{'idTask': 'task456', 'strName': 'Test Task'}]
            mock_submit.return_value = {'success': True, 'idTimesheet': 'ts789'}

            # Mock DynamoDB responses
            mock_get_task.return_value = 'task456'
            mock_write.return_value = True

            response = processor.process_event(webhook_data)
            assert response['statusCode'] == 200
            assert "Success" in response['body']

            # Verify method calls
            mock_fetch.assert_called_once_with("123")
            mock_user.assert_called_once()
            mock_auth.assert_called_once()
            mock_tasks.assert_called_once()
            mock_submit.assert_called_once()
            mock_write.assert_called_once()

