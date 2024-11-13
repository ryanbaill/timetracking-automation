import pytest
import json
import boto3
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from src.processors.timesheet.update_processor import TimeEntryProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import EXCLUDED_LABEL_IDS

class TestTimeEntryProcessor:
    """Test suite for TimeEntryProcessor class."""

    @pytest.fixture
    def mock_first_api(self):
        """Creates mock First API."""
        with patch('src.processors.timesheet.update_processor.FirstAPI') as mock:
            mock_instance = MagicMock()
            mock_instance.fetch_event.return_value = {
                'id': '12345',
                'label_ids': [4444],
                'project': {'external_id': 'proj123'}
            }
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_second_api(self):
        """Creates mock Second API."""
        with patch('src.processors.timesheet.update_processor.SecondAPI') as mock:
            mock_instance = MagicMock()
            mock_instance.authenticate.return_value = "session_id"
            mock_instance.update_timesheet.return_value = {'success': True}
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_config(self):
        """Creates a mock configuration."""
        with patch('src.processors.timesheet.update_processor.Config') as mock:
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
            yield mock_instance

    @pytest.fixture
    def mock_dynamodb(self):
        """Creates mock DynamoDB handler."""
        with patch('src.processors.timesheet.update_processor.DynamoDBHandler') as mock:
            mock_instance = MagicMock()
            mock_instance.get_task_mapping.return_value = 'Test Task'
            mock_instance.get_timesheet_entry.return_value = ('entry789', 'user456')
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_sns(self):
        """Creates mock SNS notifier."""
        with patch('src.processors.timesheet.update_processor.SNSNotifier') as mock:
            yield mock

    @pytest.fixture
    def mock_sqs(self):
        """Creates mock SQS client."""
        with patch('src.processors.timesheet.update_processor.boto3.client') as mock:
            mock_sqs = MagicMock()
            mock_sqs.get_queue_url.return_value = {'QueueUrl': 'mock-queue-url'}
            mock.return_value = mock_sqs
            yield mock

    @pytest.fixture
    def processor(self, mock_first_api, mock_second_api, mock_config, mock_dynamodb, mock_sns, mock_sqs):
        """Creates a TimeEntryProcessor instance with mocked dependencies."""
        return TimeEntryProcessor()

    def test_initialization(self, processor):
        """Tests processor initialization."""
        assert processor.first_api is not None
        assert processor.second_api is not None
        assert processor.dynamodb_handler is not None
        assert processor.sns_notifier is not None
        assert processor.queue_url is not None

    def test_process_event_success(self, processor):
        """Tests successful event processing."""
        webhook_data = {
            "payload": {
                "entity_id": "123",
                "entity_path": "timesheets/123"
            }
        }

        with patch.object(processor.first_api, 'fetch_event') as mock_fetch, \
             patch.object(processor.second_api, 'authenticate') as mock_auth, \
             patch.object(processor.second_api, 'fetch_tasks') as mock_tasks, \
             patch.object(processor.second_api, 'update_timesheet') as mock_update, \
             patch.object(processor.dynamodb_handler, 'get_task_mapping') as mock_get_task, \
             patch.object(processor.dynamodb_handler, 'get_timesheet_entry') as mock_get_entry:

            # Mock responses
            mock_fetch.return_value = {
                'id': '12345',
                'label_ids': [4444],
                'project': {'external_id': 'proj123'},
                'user': {'id': 'user123'},
                'duration': 3600,
                'timestamp': 1710864000
            }
            mock_auth.return_value = "session_id"
            mock_tasks.return_value = [{'idTask': 'task456', 'strName': 'Test Task'}]
            mock_update.return_value = {'success': True}
            mock_get_task.return_value = 'Test Task'
            mock_get_entry.return_value = ('entry789', 'user456')

            response = processor.process_event(webhook_data)
            assert response['statusCode'] == 200
            assert "Update Successful" in response['body']

            # Verify method calls
            mock_fetch.assert_called_once()
            mock_auth.assert_called_once()
            mock_tasks.assert_called_once()
            mock_update.assert_called_once()
            mock_get_task.assert_called_once()
            mock_get_entry.assert_called_once()

    def test_process_event_invalid_payload(self, processor):
        """Tests processing with invalid payload."""
        invalid_event = {"body": "{}"}
        response = processor.process_event(invalid_event)
        assert response['statusCode'] == 400
        assert "Invalid Event" in response['body']

    def test_process_event_ai_suggestion(self, processor):
        """Tests handling of AI-generated suggestions."""
        ai_event = {
            "payload": {
                "entity_id": "123",
                "entity_path": "suggested_hours/123"
            }
        }
        response = processor.process_event(ai_event)
        assert response['statusCode'] == 200
        assert "Skipped Entry" in response['body']

    def test_find_matching_task_id(self, processor):
        """Tests task ID matching functionality."""
        tasks = [
            {"idTask": "task1", "strName": "Task 1"},
            {"idTask": "task2", "strName": "Task 2"}
        ]
        
        task_id = processor.find_matching_task_id(tasks, "Task 1")
        assert task_id == "task1"
        
        task_id = processor.find_matching_task_id(tasks, "Non-existent Task")
        assert task_id is None

    def test_parse_firstservice_event(self, processor):
        """Tests event data parsing."""
        event_data = {
            'id': '12345',
            'label_ids': [4444],
            'project': {
                'external_id': 'proj123',
                'client': {'external_id': 'client123'}
            },
            'user': {'id': 'user123'},
            'duration': 3600,
            'additional_info': {'day': '2024-03-20'}
        }
        
        parsed_data = processor.parse_firstservice_event(event_data)
        
        assert parsed_data['project']['external_id'] == 'proj123'
        assert parsed_data['client']['external_id'] == 'client123'
        assert parsed_data['user']['id'] == 'user123'
        assert parsed_data['additional_info']['total_hours'] == 1.0
        assert parsed_data['additional_info']['day'] == '2024-03-20'
        assert parsed_data['additional_info']['note'] == ''
