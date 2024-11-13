import pytest
import json
from unittest.mock import patch, MagicMock, call
from src.lambda_functions.cleanup.handler import lambda_handler, retry_handler

@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = "test_function"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test_function"
    return context

@pytest.fixture
def mock_config():
    with patch('src.lambda_functions.cleanup.handler.Config') as mock_config:
        mock_instance = MagicMock()
        mock_config.return_value = mock_instance
        
        # Mock the required attributes
        mock_instance.SNS_TOPIC_ARN = 'fake:arn'
        
        yield mock_instance

def test_lambda_handler(lambda_context, mock_config):
    """Tests the main cleanup Lambda handler function."""
    with patch('src.lambda_functions.cleanup.handler.CleanupProcessor') as mock_processor, \
         patch('src.lambda_functions.cleanup.handler.SNSNotifier') as mock_sns:
        
        # Setup mocks
        mock_processor_instance = MagicMock()
        mock_processor.return_value = mock_processor_instance
        mock_processor_instance.execute_cleanup.return_value = 5

        # Test successful cleanup
        response = lambda_handler({}, lambda_context)
        
        assert response['statusCode'] == 200
        assert json.loads(response['body'])['deleted_count'] == 5
        assert "Successfully deleted 5 entries" in json.loads(response['body'])['message']
        
        # Test no entries found
        mock_processor_instance.execute_cleanup.return_value = 0
        response = lambda_handler({}, lambda_context)
        
        assert response['statusCode'] == 200
        assert json.loads(response['body'])['deleted_count'] == 0
        assert "No entries found for deletion" in json.loads(response['body'])['message']
        
        # Test error handling
        mock_processor_instance.execute_cleanup.side_effect = Exception("Test error")
        response = lambda_handler({}, lambda_context)
        
        assert response['statusCode'] == 500
        assert "Test error" in json.loads(response['body'])['error']
