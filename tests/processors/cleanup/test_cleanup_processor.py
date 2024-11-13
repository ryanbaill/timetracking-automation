import pytest
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from boto3.dynamodb.conditions import Attr
from src.processors.cleanup.cleanup_processor import CleanupProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.constants.constants import (
    TABLES,
    SQS_OPERATIONS
)

@pytest.fixture
def mock_config():
    """Creates a mock configuration."""
    with patch('src.processors.cleanup.cleanup_processor.Config') as mock:
        mock_instance = MagicMock()
        mock_instance.get_parameter.return_value = "30"
        mock_instance.SNS_TOPIC_ARN = "test:arn"
        mock_instance.SQS_QUEUE_URL = "test:url"
        mock.return_value = mock_instance
        yield mock

@pytest.fixture
def mock_dynamodb():
    """Creates a mock DynamoDB handler."""
    with patch('src.processors.cleanup.cleanup_processor.DynamoDBHandler') as mock:
        mock_instance = MagicMock()
        mock_instance.table = MagicMock()
        mock_instance.table.scan.return_value = {
            'Items': [
                {'FirstServiceEntityID': '123', 'Date': '2023-01-01'},
                {'FirstServiceEntityID': '456', 'Date': '2023-01-02'}
            ]
        }
        mock.return_value = mock_instance
        yield mock

@pytest.fixture
def mock_sns():
    """Creates a mock SNS notifier."""
    with patch('src.processors.cleanup.cleanup_processor.SNSNotifier', autospec=True) as mock:
        yield mock

@pytest.fixture
def mock_sqs():
    """Creates a mock SQS client."""
    with patch('src.processors.cleanup.cleanup_processor.SQSClient', autospec=True) as mock:
        yield mock

@pytest.fixture
def processor(mock_config, mock_dynamodb, mock_sns, mock_sqs):
    """Creates a CleanupProcessor instance with mocked dependencies."""
    with patch('src.processors.cleanup.cleanup_processor.Config', mock_config), \
         patch('src.processors.cleanup.cleanup_processor.DynamoDBHandler', mock_dynamodb), \
         patch('src.processors.cleanup.cleanup_processor.SNSNotifier', mock_sns), \
         patch('src.processors.cleanup.cleanup_processor.SQSClient', mock_sqs):
        return CleanupProcessor()

class TestCleanupProcessor:
    """Test suite for CleanupProcessor class."""

    def test_execute_cleanup_success(self, processor, mock_dynamodb):
        """Tests successful cleanup execution."""
        with patch.object(processor.dynamodb_handler, 'delete_entry', return_value=True):
            result = processor.execute_cleanup()
            assert result == 2

    def test_execute_cleanup_no_entries(self, processor, mock_dynamodb):
        """Tests cleanup with no entries to delete."""
        mock_dynamodb.return_value.table.scan.return_value = {'Items': []}
        result = processor.execute_cleanup()
        assert result == 0

    def test_execute_cleanup_partial_success(self, processor, mock_dynamodb):
        """Tests cleanup with some failed deletions."""
        with patch.object(processor.dynamodb_handler, 'delete_entry') as mock_delete:
            mock_delete.side_effect = [True, False]
            with patch.object(processor.sqs_client, 'send_message') as mock_sqs:
                result = processor.execute_cleanup()
                assert result == 1
                mock_sqs.assert_called_once()

    def test_execute_cleanup_error(self, processor):
        """Tests cleanup with scan error."""
        with patch.object(processor.dynamodb_handler, 'table') as mock_table:
            mock_table.scan.side_effect = Exception("Test error")
            with pytest.raises(Exception, match="Test error"):
                processor.execute_cleanup()

    def test_scan_old_entries_pagination(self, processor, mock_dynamodb):
        """Tests scanning with pagination."""
        mock_dynamodb.return_value.table.scan.side_effect = [
            {
                'Items': [{'FirstServiceEntityID': '123'}],
                'LastEvaluatedKey': 'key1'
            },
            {
                'Items': [{'FirstServiceEntityID': '456'}]
            }
        ]
        entries = processor._scan_old_entries('2023-01-01')
        assert len(entries) == 2

    def test_process_deletions_retry_queue(self, processor):
        """Tests deletion retry queue functionality."""
        entries = [
            {'FirstServiceEntityID': '123'},
            {'FirstServiceEntityID': '456'}
        ]
        with patch.object(processor.dynamodb_handler, 'delete_entry', return_value=False), \
             patch.object(processor.sqs_client, 'send_message') as mock_sqs:
            result = processor._process_deletions(entries)
            assert result == 0
            assert mock_sqs.call_count == 2

    def test_send_notification(self, processor):
        """Tests notification sending."""
        with patch.object(processor.sns_notifier, 'send_message') as mock_sns:
            processor._send_notification(5, '2023-01-01')
            mock_sns.assert_called_once()
            assert "Successfully deleted 5 entries" in mock_sns.call_args[0][1]
