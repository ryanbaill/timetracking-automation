import pytest
import json
import boto3
from datetime import datetime
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from src.lambda_functions.timesheet_operations.delete.handler import lambda_handler, retry_handler
from src.processors.timesheet.delete_processor import TimeEntryProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import SQS_OPERATIONS

@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = "test_function"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test_function"
    return context

@pytest.fixture
def mock_sqs():
    """Mock SQS setup."""
    with mock_aws():
        sqs = boto3.resource('sqs', region_name='us-east-1')
        queue = sqs.create_queue(QueueName='test-queue')
        dlq = sqs.create_queue(QueueName='test-dlq')
        yield sqs, queue.url, dlq.url

@pytest.fixture
def mock_apis():
    """Mock First and Second API instances."""
    with patch('src.utils.apis.first_api.FirstAPI') as mock_first_api, \
         patch('src.utils.apis.second_api.SecondAPI') as mock_second_api:
        
        first_api_instance = MagicMock()
        second_api_instance = MagicMock()
        
        mock_first_api.return_value = first_api_instance
        mock_second_api.return_value = second_api_instance
        
        first_api_instance.fetch_event.return_value = {
            'id': '12345',
            'status': 'active'
        }
        
        second_api_instance.delete_entry.return_value = True
        
        yield first_api_instance, second_api_instance

@pytest.fixture
def mock_config():
    """Mock Config setup."""
    with patch('src.utils.config.config_manager.Config') as mock_config:
        config_instance = MagicMock()
        mock_config.return_value = config_instance
        
        # Mock all necessary config values
        config_instance.API_ONE_TOKEN = 'mock_api_one_token'
        config_instance.API_TWO_TOKEN = 'mock_api_two_token'
        config_instance.API_ONE_BASE_URL = 'https://api-one.example.com'
        config_instance.API_TWO_BASE_URL = 'https://api-two.example.com'
        config_instance.DYNAMODB_TABLE = 'test-table'
        config_instance.SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:123456789012:test-topic'
        config_instance.SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue'
        config_instance.SQS_DLQ_URL = 'https://sqs.us-east-1.amazonaws.com/123456789012/test-dlq'
        config_instance.AWS_REGION = 'us-east-1'
        
        yield config_instance

@pytest.fixture
def mock_ssm():
    """Mock SSM parameter store."""
    with mock_aws():
        ssm = boto3.client('ssm', region_name='us-east-1')
        # Pre-populate all required parameters
        parameters = {
            '/api/firstservice/token': 'mock-token',
            '/api/firstservice/account_id': 'mock-account-id',
            '/api/secondservice/org_code': 'mock-org-code',
            '/api/secondservice/username': 'mock-username',
            '/api/secondservice/password': 'mock-password',
            '/api/secondservice/user_id': 'mock-user-id',
            '/notifications/sns_topic_arn': 'mock-sns-arn',
            '/sqs/queue_url': 'mock-sqs-url'
        }
        
        for param_name, param_value in parameters.items():
            ssm.put_parameter(
                Name=param_name,
                Value=param_value,
                Type='SecureString'
            )
        yield ssm

@pytest.fixture
def sample_webhook_event():
    return {
        'body': json.dumps({
            'timesheet_id': '12345',
            'operation': 'delete',
            'first_service_id': '123',
            'second_service_id': 'ext123'
        })
    }

@mock_aws
def test_lambda_handler(sample_webhook_event, lambda_context, mock_apis, mock_config, mock_ssm):
    """Tests the main Lambda handler function."""
    with patch('src.lambda_functions.timesheet_operations.delete.handler.TimeEntryProcessor') as mock_processor:
        # Configure TimeEntryProcessor mock
        processor_instance = MagicMock()
        mock_processor.return_value = processor_instance
        
        # Configure success case response
        processor_instance.process_event.return_value = {
            'statusCode': 200,
            'body': json.dumps({
                'content': {
                    'title': 'Deletion Successful',
                    'message': 'Time entry deleted successfully'
                }
            })
        }

        # Test successful invocation
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 200
        processor_instance.process_event.assert_called_once_with(sample_webhook_event)

        # Configure error case response
        processor_instance.process_event.side_effect = Exception("Test error")
        processor_instance.handle_error.return_value = {
            'statusCode': 500,
            'body': json.dumps({'error': 'Test error'})
        }
        
        # Test error case
        response = lambda_handler(sample_webhook_event, lambda_context)
        assert response['statusCode'] == 500

@mock_aws
def test_retry_handler(mock_sqs, lambda_context, mock_config):
    """Tests the retry Lambda handler function."""
    with patch('src.lambda_functions.timesheet_operations.delete.handler.Config', return_value=mock_config), \
         patch('src.lambda_functions.timesheet_operations.delete.handler.boto3.resource') as mock_resource:
        
        sqs, queue_url, dlq_url = mock_sqs
        
        # Set up the mock DynamoDB table
        mock_table = MagicMock()
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_resource.return_value = mock_dynamodb

        # Create a sample retry event
        retry_event = {
            'Records': [
                {
                    'body': json.dumps({
                        'operation': 'delete_entry', 
                        'data': {'FirstServiceEntityID': 12345}
                    })
                }
            ]
        }

        # Test successful retry
        response = retry_handler(retry_event, lambda_context)
        
        # Verify the delete_item was called with correct parameters
        mock_table.delete_item.assert_called_once_with(
            Key={'FirstServiceEntityID': 12345}
        )
        assert response['statusCode'] == 200
        assert 'Retry operation successful' in json.loads(response['body'])['message']

        # Test error handling
        mock_table.delete_item.side_effect = Exception("Test error")
        response = retry_handler(retry_event, lambda_context)
        assert response['statusCode'] == 500
        assert 'Test error' in json.loads(response['body'])['error']
