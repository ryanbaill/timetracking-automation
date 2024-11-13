import pytest
import json
import boto3
from datetime import datetime
from typing import Dict, Any
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from src.lambda_functions.backup_operations.delete.handler import lambda_handler, retry_handler
from src.processors.backup.delete_processor import BackupProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.constants.constants import (
    TABLES,
    SQS_OPERATIONS,
    DEFAULT_TIMEOUT
)

@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = "backup-delete-function"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:backup-delete"
    return context

def test_lambda_handler(sample_webhook_event, lambda_context, mock_apis):
    """Tests the main Lambda handler function."""
    with patch('src.lambda_functions.backup_operations.delete.handler.Config'), \
         patch('src.lambda_functions.backup_operations.delete.handler.DynamoDBHandler') as mock_dynamodb, \
         patch('src.lambda_functions.backup_operations.delete.handler.SNSNotifier'), \
         patch('src.lambda_functions.backup_operations.delete.handler.boto3'):

        mock_dynamodb.return_value.get_backup_entry.return_value = (123, 'ext123')
        mock_dynamodb.return_value.delete_backup_entry.return_value = True
        mock_first_api, _ = mock_apis

        # Test successful invocation
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 200
        assert 'Backup deletion successful' in json.loads(response['body'])['message']

        # Test invalid payload
        invalid_event = {"body": json.dumps({"invalid": "payload"})}
        response = lambda_handler(invalid_event, lambda_context)
        assert response['statusCode'] == 400
        assert 'Invalid Event' in json.loads(response['body'])['message']

        # Test entry not found
        mock_dynamodb.return_value.get_backup_entry.return_value = (None, None)
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 404
        assert 'Backup entry not found' in json.loads(response['body'])['message']

        # Test failed deletion
        mock_dynamodb.return_value.delete_backup_entry.return_value = False
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 500
        assert 'Failed to delete backup' in json.loads(response['body'])['message']

