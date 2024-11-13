import pytest
import json
import boto3
from unittest.mock import MagicMock, patch
from moto import mock_aws
from datetime import datetime

@pytest.fixture
def mock_ssm():
    """Creates a mock SSM client with test parameters."""
    with mock_aws():
        ssm = boto3.client('ssm', region_name='us-east-1')
        test_params = {
            '/api/firstservice/token': 'test_token',
            '/api/firstservice/account_id': 'test_account',
            '/api/secondservice/org_code': 'test_org',
            '/api/secondservice/username': 'test_user',
            '/api/secondservice/password': 'test_pass',
            '/api/secondservice/user_id': 'test_id',
            '/notifications/sns_topic_arn': 'test:arn:sns:topic',
            '/sqs/queue_url': 'https://sqs.test.amazonaws.com/test-queue',
            '/db/retention/days': '45'
        }
        
        for param_name, param_value in test_params.items():
            ssm.put_parameter(
                Name=param_name,
                Value=param_value,
                Type='SecureString'
            )
        yield ssm

@pytest.fixture
def mock_dynamodb():
    """Creates mock DynamoDB tables."""
    with mock_aws():
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Create Timesheet Entries table
        dynamodb.create_table(
            TableName='Timesheet_Entries',
            KeySchema=[{'AttributeName': 'FirstServiceEntityID', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'FirstServiceEntityID', 'AttributeType': 'N'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Create Labels/Tasks mapping table
        dynamodb.create_table(
            TableName='FirstService_Labels_SecondService_Task_IDs',
            KeySchema=[{'AttributeName': 'FirstServiceLabelID', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'FirstServiceLabelID', 'AttributeType': 'N'}],
            BillingMode='PAY_PER_REQUEST'
        )
        
        yield dynamodb

@pytest.fixture
def mock_sqs():
    """Creates mock SQS queues."""
    with mock_aws():
        sqs = boto3.client('sqs', region_name='us-east-1')
        
        # Create main queue
        queue = sqs.create_queue(QueueName='timesheet-queue')
        queue_url = queue['QueueUrl']
        
        # Create DLQ
        dlq = sqs.create_queue(QueueName='timesheet-dlq')
        dlq_url = dlq['QueueUrl']
        
        yield sqs, queue_url, dlq_url

@pytest.fixture
def mock_sns():
    """Creates mock SNS topic."""
    with mock_aws():
        sns = boto3.client('sns', region_name='us-east-1')
        topic = sns.create_topic(Name='test-topic')
        yield sns, topic['TopicArn']

@pytest.fixture
def sample_webhook_event():
    """Creates a sample webhook event."""
    return {
        "body": json.dumps({
            "payload": {
                "entity_id": "12345",
                "entity_path": "events/12345"
            }
        })
    }

@pytest.fixture
def sample_first_api_response():
    """Sample response from first service API."""
    return {
        "id": 12345,
        "user": {
            "id": 67890,
            "name": "Test User",
            "external_id": "ext123"
        },
        "project": {
            "external_id": "proj123",
            "name": "Test Project",
            "client": {
                "external_id": "client123",
                "name": "Test Client"
            }
        },
        "label_ids": [4018299],
        "duration": 3600,  # 1 hour in seconds
        "timestamp": int(datetime.now().timestamp()),
        "note": "Test timesheet entry",
        "updated_at": int(datetime.now().timestamp())
    }

@pytest.fixture
def sample_second_api_response():
    """Sample response from second service API."""
    return {
        "appID": "test_app_id",
        "success": True,
        "listTasks": {
            "hdr": {
                "idTask": 0,
                "strName": 1
            },
            "data": [
                [123, "Test Task"]
            ]
        },
        "listClients": {
            "data": [
                [1, "Client1", "Test Client"],
                [2, "Client2", "Another Client"]
            ]
        },
        "listJobs": {
            "data": [
                [1, "Job1", "Test Job", "Client1"],
                [2, "Job2", "Another Job", "Client2"]
            ]
        }
    }

@pytest.fixture
def mock_apis(sample_first_api_response, sample_second_api_response):
    """Mocks both API clients."""
    with patch('src.utils.apis.first_api.FirstAPI') as mock_first_api, \
         patch('src.utils.apis.second_api.SecondAPI') as mock_second_api:
        
        # Configure FirstAPI mock
        mock_first_api.return_value.fetch_event.return_value = sample_first_api_response
        mock_first_api.return_value.fetch_user.return_value = sample_first_api_response['user']
        
        # Configure SecondAPI mock
        mock_second_api.return_value.authenticate.return_value = sample_second_api_response['appID']
        mock_second_api.return_value.fetch_tasks.return_value = sample_second_api_response['listTasks']
        
        yield mock_first_api, mock_second_api
