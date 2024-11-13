import pytest
import json
from typing import Dict, Any
from unittest.mock import patch, MagicMock
from src.processors.backup.delete_processor import EventProcessor

@pytest.fixture
def sample_webhook_event():
    """Creates a sample webhook event."""
    return {
        "body": json.dumps({
            "payload": {
                "id": "123",
                "entity_path": "events/123"
            }
        })
    }

@pytest.fixture
def sample_invalid_event():
    """Creates an invalid webhook event with empty payload."""
    return {
        "body": json.dumps({
            "payload": {}  # Empty payload but it exists
        })
    }

@pytest.fixture
def sample_missing_payload_event():
    """Creates an event with missing payload field."""
    return {
        "body": json.dumps({})  # No payload field at all
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

    def test_validate_data_success(self, sample_webhook_event):
        """Tests successful data validation."""
        processor = EventProcessor(sample_webhook_event)
        result = processor.validate_data()
        assert result == {'EntityID': 123}

    def test_validate_data_missing_payload(self, sample_missing_payload_event):
        """Tests validation with missing payload."""
        processor = EventProcessor(sample_missing_payload_event)
        with pytest.raises(ValueError, match="Missing payload in event data"):
            processor.validate_data()

    def test_validate_data_missing_id(self, sample_invalid_event):
        """Tests validation with missing ID."""
        processor = EventProcessor(sample_invalid_event)
        with pytest.raises(ValueError, match="Missing required field: id"):
            processor.validate_data()

    def test_validate_data_invalid_json(self):
        """Tests validation with invalid JSON."""
        event = {"body": "invalid json"}
        processor = EventProcessor(event)
        with pytest.raises(ValueError, match="Invalid JSON format in event body"):
            processor.validate_data()

    def test_validate_data_direct_payload(self):
        """Tests validation with direct payload (no body)."""
        event = {
            "payload": {
                "id": "456",
                "entity_path": "events/456"
            }
        }
        processor = EventProcessor(event)
        result = processor.validate_data()
        assert result == {'EntityID': 456}
