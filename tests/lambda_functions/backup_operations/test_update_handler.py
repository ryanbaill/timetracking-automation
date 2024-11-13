import pytest
import json
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.lambda_functions.backup_operations.update.handler import lambda_handler
from src.processors.backup.entry_processor import EventProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI

@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = "backup-update-function"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:backup-update"
    return context

def test_lambda_handler(sample_webhook_event, lambda_context, mock_apis):
    """Tests the main Lambda handler function."""
    with patch('src.lambda_functions.backup_operations.update.handler.Config'), \
         patch('src.lambda_functions.backup_operations.update.handler.DynamoDBHandler') as mock_dynamodb, \
         patch('src.lambda_functions.backup_operations.update.handler.SNSNotifier'), \
         patch('src.lambda_functions.backup_operations.update.handler.FirstAPI') as MockFirstAPI, \
         patch('src.lambda_functions.backup_operations.update.handler.EventProcessor') as MockEventProcessor, \
         patch('src.lambda_functions.backup_operations.update.handler.SQSClient') as mock_sqs:

        # Mock DynamoDB operations
        mock_dynamodb.return_value.update_entry.return_value = True

        # Set up the mock event processor
        mock_processor = MagicMock()
        mock_processor.extract_data.return_value = {
            'EntityID': 12345,
            'date_added': datetime.now().strftime('%Y-%m-%d'),
        }
        MockEventProcessor.return_value = mock_processor

        # Mock FirstAPI response
        mock_first_api, _ = mock_apis
        mock_first_api.fetch_event.return_value = {
            'id': 12345,
            'user': {'name': 'Test User'},
            'updated_at': int(datetime.now().timestamp())
        }
        MockFirstAPI.return_value = mock_first_api

        # Mock SQS
        mock_sqs.return_value.send_message.return_value = True

        # Test successful update
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 200
        assert 'Timesheet backup update successful' in json.loads(response['body'])['message']

        # Test failed update
        def test_failed_update():
            mock_dynamodb.reset_mock()
            MockFirstAPI.reset_mock()
            MockEventProcessor.reset_mock()
            mock_sqs.reset_mock()
            
            # Configure mock to simulate update failure
            mock_dynamodb.return_value.update_entry.return_value = False
            mock_sqs.return_value.send_message.return_value = False
            
            # Reconfigure necessary mocks
            mock_processor = MagicMock()
            mock_processor.extract_data.return_value = {
                'EntityID': 12345,
                'date_added': datetime.now().strftime('%Y-%m-%d'),
            }
            MockEventProcessor.return_value = mock_processor
            
            mock_first_api.fetch_event.return_value = {
                'id': 12345,
                'user': {'name': 'Test User'},
                'updated_at': int(datetime.now().timestamp())
            }
            MockFirstAPI.return_value = mock_first_api
            
            response = lambda_handler(sample_webhook_event, lambda_context)
            assert response['statusCode'] == 500
            assert 'error' in json.loads(response['body'])

        test_failed_update()
