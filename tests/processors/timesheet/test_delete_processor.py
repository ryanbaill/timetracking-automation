import pytest
import json
import logging
import traceback
import boto3
from datetime import datetime
from typing import Dict, Optional, Any
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from src.processors.timesheet.delete_processor import TimeEntryProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI

class TestTimeEntryProcessor:
    """Test suite for TimeEntryProcessor class."""

    @pytest.fixture
    def mock_first_api(self):
        """Creates mock First API."""
        with patch('src.processors.timesheet.delete_processor.FirstAPI') as mock:
            mock_instance = MagicMock()
            mock_instance.fetch_event.return_value = {"id": 123}
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_second_api(self):
        """Creates mock Second API."""
        with patch('src.processors.timesheet.delete_processor.SecondAPI') as mock:
            mock_instance = MagicMock()
            mock_instance.authenticate.return_value = "session_id"
            mock_instance.delete_timesheet.return_value = True
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_config(self):
        """Creates a mock configuration."""
        with patch('src.processors.timesheet.delete_processor.Config') as mock:
            mock_instance = MagicMock()
            mock_instance.API_ONE_TOKEN = 'mock_api_one_token'
            mock_instance.API_TWO_TOKEN = 'mock_api_two_token'
            mock_instance.API_ONE_BASE_URL = 'https://api-one.example.com'
            mock_instance.API_TWO_BASE_URL = 'https://api-two.example.com'
            mock_instance.DYNAMODB_TABLE = 'test-table'
            mock_instance.SNS_TOPIC_ARN = 'test:arn'
            mock_instance.SQS_QUEUE_URL = 'test:url'
            mock_instance.AWS_REGION = 'us-east-1'
            mock.return_value = mock_instance
            yield mock

    @pytest.fixture
    def mock_dynamodb(self):
        """Creates mock DynamoDB handler."""
        with patch('src.processors.timesheet.delete_processor.DynamoDBHandler') as mock:
            mock_instance = MagicMock()
            mock_instance.get_timesheet_entry.return_value = (456, "ext789")
            mock_instance.delete_entry.return_value = True
            mock.return_value = mock_instance
            yield mock

    @pytest.fixture
    def mock_sns(self):
        """Creates mock SNS notifier."""
        with patch('src.processors.timesheet.delete_processor.SNSNotifier', autospec=True) as mock:
            yield mock

    @pytest.fixture
    def processor(self, mock_config, mock_dynamodb, mock_sns, mock_first_api, mock_second_api):
        """Creates a TimeEntryProcessor instance with mocked dependencies."""
        with patch('src.processors.timesheet.delete_processor.Config', mock_config), \
             patch('src.processors.timesheet.delete_processor.DynamoDBHandler', mock_dynamodb), \
             patch('src.processors.timesheet.delete_processor.SNSNotifier', mock_sns), \
             patch('src.processors.timesheet.delete_processor.FirstAPI', mock_first_api), \
             patch('src.processors.timesheet.delete_processor.SecondAPI', mock_second_api):
            return TimeEntryProcessor()

    def test_initialization(self, processor):
        """Tests processor initialization."""
        assert processor.first_api is not None
        assert processor.second_api is not None
        assert processor.dynamodb_handler is not None
        assert processor.sns_notifier is not None

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
             patch.object(processor.second_api, 'delete_timesheet') as mock_delete, \
             patch.object(processor.dynamodb_handler, 'get_timesheet_entry') as mock_get, \
             patch.object(processor.dynamodb_handler, 'delete_entry') as mock_db_delete:

            mock_fetch.return_value = {"id": 123}
            mock_auth.return_value = "session_id"
            mock_delete.return_value = True
            mock_get.return_value = (456, "ext789")
            mock_db_delete.return_value = True

            response = processor.process_event(webhook_data)
            assert response['statusCode'] == 200
            assert "Deletion Successful" in response['body']

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
        assert "Skipped Deletion" in response['body']

    def test_process_event_not_found(self, processor):
        """Tests handling of non-existent entries."""
        webhook_data = {
            "payload": {
                "entity_id": "123",
                "entity_path": "timesheets/123"
            }
        }

        with patch.object(processor.first_api, 'fetch_event') as mock_fetch:
            mock_fetch.return_value = None
            response = processor.process_event(webhook_data)
            assert response['statusCode'] == 200
            assert "Event not found" in response['body']

    def test_process_event_deletion_failure(self, processor):
        """Tests handling of deletion failures."""
        webhook_data = {
            "payload": {
                "entity_id": "123",
                "entity_path": "timesheets/123"
            }
        }

        with patch.object(processor.first_api, 'fetch_event') as mock_fetch, \
             patch.object(processor.second_api, 'authenticate') as mock_auth, \
             patch.object(processor.second_api, 'delete_timesheet') as mock_delete, \
             patch.object(processor.dynamodb_handler, 'get_timesheet_entry') as mock_get:

            mock_fetch.return_value = {"id": 123}
            mock_auth.return_value = "session_id"
            mock_delete.return_value = False
            mock_get.return_value = (456, "ext789")

            response = processor.process_event(webhook_data)
            assert response['statusCode'] == 200
            assert "Failed to delete timesheet" in response['body']

