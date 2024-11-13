import boto3
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class ConfigManager:
    """Manages configuration retrieval from AWS Systems Manager Parameter Store."""
    def __init__(self):
        self.ssm = boto3.client('ssm', region_name='us-east-1')
        self._config = {}

    def get_parameter(self, param_name, decrypt=True):
        """Retrieves a parameter from SSM Parameter Store."""
        if param_name not in self._config:
            try:
                response = self.ssm.get_parameter(
                    Name=param_name,
                    WithDecryption=decrypt
                )
                self._config[param_name] = response['Parameter']['Value']
            except Exception as e:
                logger.error(f"Error fetching parameter {param_name}: {str(e)}")
                raise
        return self._config[param_name]

class Config:
    """Configuration constants retrieved from Parameter Store."""
    def __init__(self):
        self.config_manager = ConfigManager()
        
        # API One parameters
        self.API_ONE_TOKEN = self.config_manager.get_parameter('/api/firstservice/token')
        self.API_ONE_ACCOUNT_ID = self.config_manager.get_parameter('/api/firstservice/account_id')
        
        # API Two parameters
        self.API_TWO_ORG_CODE = self.config_manager.get_parameter('/api/secondservice/org_code')
        self.API_TWO_USERNAME = self.config_manager.get_parameter('/api/secondservice/username')
        self.API_TWO_PASSWORD = self.config_manager.get_parameter('/api/secondservice/password')
        self.API_TWO_USER_ID = self.config_manager.get_parameter('/api/secondservice/user_id')
        
        # SNS configuration
        self.SNS_TOPIC_ARN = self.config_manager.get_parameter('/notifications/sns_topic_arn')
        
        # SQS configuration
        self.SQS_QUEUE_URL = self.config_manager.get_parameter('/sqs/queue_url')
