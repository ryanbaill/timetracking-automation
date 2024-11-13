import pytest
import json
import boto3
from datetime import datetime, UTC
from typing import Dict, List, Any
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from src.lambda_functions.job_client_operations.entry.handler import lambda_handler, retry_handler
from src.processors.job_client.entry_processor import JobProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import (
    EXCLUDED_CLIENTS,
    DEFAULT_PROJECT_COLOR,
    DEFAULT_RATE_TYPE,
    DEFAULT_USER_IDS,
    DEFAULT_LABEL_IDS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS
)

@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = "job-client-sync-function"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:job-client-sync"
    return context

@pytest.fixture
def mock_apis():
    """Mock First and Second API instances."""
    with patch('src.utils.apis.first_api.FirstAPI') as mock_first_api, \
         patch('src.utils.apis.second_api.SecondAPI') as mock_second_api:
        
        first_api_instance = MagicMock()
        second_api_instance = MagicMock()
        
        mock_first_api.return_value = first_api_instance
        mock_second_api.return_value = second_api_instance
        
        # Configure mock responses
        first_api_instance.get_clients.return_value = [
            {'id': 1, 'name': 'Client 1'},
            {'id': 2, 'name': 'Client 2'}
        ]
        
        second_api_instance.authenticate.return_value = "session_token"
        second_api_instance.create_client.return_value = True
        
        yield first_api_instance, second_api_instance

def test_lambda_handler_success(lambda_context, mock_apis):
    """Tests successful synchronization."""
    with patch('src.lambda_functions.job_client_operations.entry.handler.Config'), \
         patch('src.lambda_functions.job_client_operations.entry.handler.SNSNotifier'), \
         patch('src.lambda_functions.job_client_operations.entry.handler.SQSClient'), \
         patch('src.lambda_functions.job_client_operations.entry.handler.JobProcessor') as mock_processor, \
         patch('src.lambda_functions.job_client_operations.entry.handler.FirstAPI') as mock_first_api_class, \
         patch('src.lambda_functions.job_client_operations.entry.handler.SecondAPI') as mock_second_api_class:
        
        # Configure mock APIs to use the fixture's instances
        first_api_instance, second_api_instance = mock_apis
        mock_first_api_class.return_value = first_api_instance
        mock_second_api_class.return_value = second_api_instance
        
        # Configure processor mock
        processor_instance = MagicMock()
        mock_processor.return_value = processor_instance
        processor_instance.process_clients.return_value = {
            'success': True,
            'clients': ['client1', 'client2'],
            'results': []
        }
        processor_instance.process_projects.return_value = {
            'success': True,
            'results': []
        }
        
        # Test successful execution
        response = lambda_handler({}, lambda_context)
        assert response['statusCode'] == 200
        assert 'Synchronization completed successfully' in json.loads(response['body'])['message']

def test_lambda_handler_failure(lambda_context, mock_apis):
    """Tests synchronization failure scenarios."""
    with patch('src.lambda_functions.job_client_operations.entry.handler.Config') as mock_config, \
         patch('src.lambda_functions.job_client_operations.entry.handler.SNSNotifier'), \
         patch('src.lambda_functions.job_client_operations.entry.handler.SQSClient'), \
         patch('src.lambda_functions.job_client_operations.entry.handler.FirstAPI') as mock_first_api_class, \
         patch('src.lambda_functions.job_client_operations.entry.handler.SecondAPI') as mock_second_api_class, \
         patch('src.lambda_functions.job_client_operations.entry.handler.JobProcessor') as mock_processor:
        
        # Configure mock APIs
        first_api_instance, second_api_instance = mock_apis
        mock_first_api_class.return_value = first_api_instance
        mock_second_api_class.return_value = second_api_instance
        
        # Configure second API to fail authentication
        second_api_instance.authenticate.side_effect = Exception("Client sync failed")
        
        # Configure processor mock
        processor_instance = MagicMock()
        mock_processor.return_value = processor_instance
        processor_instance.process_clients.return_value = {
            'success': False,
            'error': 'Client sync failed'
        }
        
        response = lambda_handler({}, lambda_context)
        assert response['statusCode'] == 500
        assert 'Client sync failed' in json.loads(response['body'])['error']

def test_retry_handler(lambda_context):
    """Tests the retry handler functionality."""
    with patch('src.lambda_functions.job_client_operations.entry.handler.Config'), \
         patch('src.lambda_functions.job_client_operations.entry.handler.FirstAPI') as mock_first_api:
        
        first_api_instance = MagicMock()
        mock_first_api.return_value = first_api_instance
        
        retry_event = {
            'Records': [
                {
                    'body': json.dumps({
                        'operation': 'create_client',
                        'data': {'name': 'Test Client'}
                    })
                }
            ]
        }
        
        response = retry_handler(retry_event, lambda_context)
        assert response['statusCode'] == 200
        assert 'Retry operations completed successfully' in json.loads(response['body'])['message']
        first_api_instance.create_client.assert_called_once_with({'name': 'Test Client'})

