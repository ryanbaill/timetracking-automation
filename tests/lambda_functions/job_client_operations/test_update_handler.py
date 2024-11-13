import pytest
import json
import boto3
from datetime import datetime, UTC
from typing import Dict, List, Any
from unittest.mock import patch, MagicMock, call
from moto import mock_aws
from src.lambda_functions.job_client_operations.update.handler import lambda_handler, retry_handler
from src.processors.job_client.update_processor import JobUpdateProcessor
from src.utils.config.config_manager import Config
from src.utils.aws.dynamodb import DynamoDBHandler
from src.utils.aws.sns import SNSNotifier
from src.utils.aws.sqs import SQSClient
from src.utils.apis.first_api import FirstAPI
from src.utils.apis.second_api import SecondAPI
from src.utils.constants.constants import (
    EXCLUDED_CLIENTS,
    DEFAULT_TIMEOUT,
    SQS_OPERATIONS,
    TABLES
)

@pytest.fixture
def lambda_context():
    """Mock Lambda context object."""
    context = MagicMock()
    context.function_name = "job-client-update-function"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:job-client-update"
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
        first_api_instance.get_jobs.return_value = [
            {'id': 1, 'name': 'Job 1'},
            {'id': 2, 'name': 'Job 2'}
        ]
        
        second_api_instance.authenticate.return_value = "session_token"
        second_api_instance.update_job.return_value = True
        
        yield first_api_instance, second_api_instance

def test_lambda_handler_success(lambda_context, mock_apis):
    """Tests successful job update process."""
    with patch('src.lambda_functions.job_client_operations.update.handler.Config'), \
         patch('src.lambda_functions.job_client_operations.update.handler.DynamoDBHandler') as mock_dynamodb, \
         patch('src.lambda_functions.job_client_operations.update.handler.SNSNotifier'), \
         patch('src.lambda_functions.job_client_operations.update.handler.SQSClient'), \
         patch('src.lambda_functions.job_client_operations.update.handler.JobUpdateProcessor') as mock_processor:

        # Configure processor mock
        processor_instance = MagicMock()
        mock_processor.return_value = processor_instance
        processor_instance.process_changes.return_value = {
            'success': True,
            'updated': 2,
            'deleted': 1,
            'orphaned': 0
        }

        # Test successful execution
        response = lambda_handler({}, lambda_context)
        assert response['statusCode'] == 200
        assert 'Update process completed successfully' in json.loads(response['body'])['message']
        assert json.loads(response['body'])['updates'] == 2
        assert json.loads(response['body'])['deletions'] == 1

def test_lambda_handler_failure(lambda_context, mock_apis):
    """Tests update process failure scenarios."""
    with patch('src.lambda_functions.job_client_operations.update.handler.Config'), \
         patch('src.lambda_functions.job_client_operations.update.handler.DynamoDBHandler'), \
         patch('src.lambda_functions.job_client_operations.update.handler.SNSNotifier') as mock_sns, \
         patch('src.lambda_functions.job_client_operations.update.handler.SQSClient'), \
         patch('src.lambda_functions.job_client_operations.update.handler.JobUpdateProcessor') as mock_processor:

        # Configure processor mock for failure
        processor_instance = MagicMock()
        mock_processor.return_value = processor_instance
        processor_instance.process_changes.return_value = {
            'success': False,
            'error': 'Update failed'
        }

        # Test failed execution
        response = lambda_handler({}, lambda_context)
        assert response['statusCode'] == 500
        assert 'Update failed' in json.loads(response['body'])['error']
        mock_sns.return_value.send_message.assert_called_once()

def test_retry_handler(lambda_context):
    """Tests the retry handler functionality."""
    with patch('src.lambda_functions.job_client_operations.update.handler.boto3') as mock_boto3:
        mock_table = MagicMock()
        mock_boto3.resource.return_value.Table.return_value = mock_table

        # Test update operation
        update_event = {
            'Records': [{
                'body': json.dumps({
                    'operation': 'update_job',
                    'data': {'JobID': '123', 'name': 'Updated Job'}
                })
            }]
        }
        response = retry_handler(update_event, lambda_context)
        assert response['statusCode'] == 200
        mock_table.put_item.assert_called_once()

        # Test delete operation
        delete_event = {
            'Records': [{
                'body': json.dumps({
                    'operation': 'delete_job',
                    'data': {'JobID': '123'}
                })
            }]
        }
        mock_table.reset_mock()
        response = retry_handler(delete_event, lambda_context)
        assert response['statusCode'] == 200
        mock_table.delete_item.assert_called_once()

        # Test error handling
        mock_table.put_item.side_effect = Exception("DynamoDB error")
        response = retry_handler(update_event, lambda_context)
        assert response['statusCode'] == 500
        assert 'DynamoDB error' in json.loads(response['body'])['error']
