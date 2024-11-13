import pytest
import json
from datetime import datetime
from typing import Dict, Any
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
import boto3
from src.processors.backup.entry_processor import EventProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.constants.constants import (
    TABLES,
    SQS_OPERATIONS
)

@pytest.fixture
def sample_webhook_event():
    """Creates a sample webhook event."""
    return {
        "body": json.dumps({
            "payload": {
                "id": "123",
                "user": {"name": "John Doe"},
                "project": {
                    "name": "Test Project",
                    "client": {"name": "Test Client"}
                },
                "duration": {
                    "hours": 2,
                    "minutes": 30
                },
                "note": "Test note",
                "label_ids": ["label1"],
                "updated_at": 1234567890
            }
        })
    }

@pytest.fixture
def sample_invalid_event():
    """Creates an invalid webhook event."""
    return {
        "body": json.dumps({
            "payload": {}
        })
    }

class TestEventProcessor:
    """Test suite for EventProcessor class."""

    def test_initialization(self, sample_webhook_event):
        """Tests processor initialization."""
        processor = EventProcessor(sample_webhook_event)
        assert processor.event_data == sample_webhook_event

    def test_initialization_empty_event(self):
        """Tests initialization with empty event data."""
        with pytest.raises(ValueError, match="Event data cannot be empty"):
            EventProcessor(None)

    def test_extract_data_success(self, sample_webhook_event):
        """Tests successful data extraction."""
        processor = EventProcessor(sample_webhook_event)
        result = processor.extract_data()
        
        assert result['EntityID'] == 123
        assert isinstance(result['date_added'], str)
        assert result['user_name'] == "John Doe"
        assert result['project_name'] == "Test Project"
        assert result['client_name'] == "Test Client"
        assert result['hours'] == 2
        assert result['minutes'] == 30
        assert result['note'] == "Test note"
        assert result['label_id'] == "label1"
        assert result['updated_at'] == 1234567890

    def test_extract_data_missing_payload(self):
        """Tests extraction with missing payload."""
        event = {"body": json.dumps({})}
        processor = EventProcessor(event)
        with pytest.raises(ValueError, match="Missing payload in event data"):
            processor.extract_data()

    def test_extract_data_missing_id(self, sample_invalid_event):
        """Tests extraction with missing ID."""
        processor = EventProcessor(sample_invalid_event)
        with pytest.raises(ValueError, match="Missing required field: id"):
            processor.extract_data()

    def test_extract_data_invalid_json(self):
        """Tests extraction with invalid JSON."""
        event = {"body": "invalid json"}
        processor = EventProcessor(event)
        with pytest.raises(ValueError, match="Invalid JSON format in event body"):
            processor.extract_data()

    def test_extract_data_with_defaults(self):
        """Tests extraction with missing optional fields."""
        event = {
            "body": json.dumps({
                "payload": {
                    "id": "123"
                }
            })
        }
        processor = EventProcessor(event)
        result = processor.extract_data()
        
        assert result['EntityID'] == 123
        assert result['user_name'] == ""
        assert result['project_name'] == ""
        assert result['client_name'] == ""
        assert result['hours'] == 0
        assert result['minutes'] == 0
        assert result['note'] == ""
        assert result['label_id'] is None
        assert result['updated_at'] == 0
