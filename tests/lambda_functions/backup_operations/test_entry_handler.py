import pytest
import json
from unittest.mock import MagicMock, patch
from src.lambda_functions.backup_operations.entry.handler import lambda_handler
from src.processors.backup.entry_processor import EventProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.apis.first_api import FirstAPI
from src.utils.aws.sqs import SQSClient

@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = "backup-entry-function"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:backup-entry"
    return context

def test_lambda_handler(sample_webhook_event, lambda_context, mock_apis):
    """Tests the main Lambda handler function."""
    with patch('src.lambda_functions.backup_operations.entry.handler.Config'), \
         patch('src.lambda_functions.backup_operations.entry.handler.DynamoDBHandler') as mock_dynamodb, \
         patch('src.lambda_functions.backup_operations.entry.handler.SNSNotifier'), \
         patch('src.lambda_functions.backup_operations.entry.handler.FirstAPI') as MockFirstAPI, \
         patch('src.lambda_functions.backup_operations.entry.handler.SQSClient'), \
         patch('src.lambda_functions.backup_operations.entry.handler.boto3'):

        # Mock DynamoDB operations
        mock_dynamodb.return_value.write_timesheet_entry.return_value = True

        # Mock FirstAPI to return a proper dictionary
        mock_first_api, _ = mock_apis
        mock_first_api.fetch_event.return_value = {
            "id": "12345",
            "entity_id": "12345",
            "entity_path": "events/12345",
            # Add other required fields here
        }
        MockFirstAPI.return_value = mock_first_api

        # Test successful invocation
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 200
        assert 'Timesheet backup successful' in json.loads(response['body'])['message']

        # Test invalid payload
        invalid_event = {"body": json.dumps({"invalid": "payload"})}
        response = lambda_handler(invalid_event, lambda_context)
        assert response['statusCode'] == 500
        assert 'error' in json.loads(response['body'])

        # Test failed DynamoDB write
        mock_dynamodb.return_value.write_timesheet_entry.side_effect = Exception("Failed to write to DynamoDB")
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 500
        assert 'error' in json.loads(response['body'])
