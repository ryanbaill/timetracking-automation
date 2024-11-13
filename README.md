# Time tracking automation

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![AWS API Gateway](https://img.shields.io/badge/AWS-API%20Gateway-FF9900)
![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-FF9900)
![AWS DynamoDB](https://img.shields.io/badge/AWS-DynamoDB-232F3E)
![AWS SNS](https://img.shields.io/badge/AWS-SNS-FF9900)
![AWS SQS](https://img.shields.io/badge/AWS-SQS-FF9900)
![AWS EventBridge](https://img.shields.io/badge/AWS-EventBridge-FF9900)
![AWS Chatbot](https://img.shields.io/badge/AWS-Chatbot-36C5F0)
![AWS IAM](https://img.shields.io/badge/AWS-IAM-232F3E)
![Pytest](https://img.shields.io/badge/Test-Pytest-7289DA)

## Table of Contents
- [Description](#description)
- [Prerequisites](#prerequisites)
- [Architecture](#architecture)
- [Components](#components)
  - [API Gateway](#api-gateway)
  - [EventBridge](#eventbridge)
  - [Lambda Functions](#lambda-functions)
  - [DynamoDB](#dynamodb)
  - [Simple Notification Service (SNS)](#simple-notification-service-sns)
  - [Simple Queue Service (SQS)](#simple-queue-service-sqs)
  - [AWS Chatbot](#aws-chatbot)
- [Error Logging](#error-logging)
- [FirstService API details](#firstservice-api-details)
- [SecondService API details](#secondservice-api-details)
- [Testing](#testing)

---

## Description
This repository contains code for automations between FirstService and SecondService. FirstService is an AI-based time-tracking system designed to help make time-tracking easier for employees. SecondService is a time-tracking reporting and analytics system used to track retainers and generate quotes based on collected data. 

SecondService is the platform used by the admin team, while the rest of the team uses FirstService to track their time. Therefore, project and client data must be relayed from SecondService to FirstService to ensure proper client and project setup. While timesheet data needs to be relayed from FirstService to SecondService for accurate reporting and quoting.

FirstService's API is robust and well-documented, while SecondService's API is outdated and poorly documented. Using SecondService's API requires significant workarounds.

## Codebase Structure
The repository is organized into modular components to maximize code reuse and maintainability:

- `src/`: Contains the core logic broken down into:
  - `processors/`: logic and data processing components
  - `lambda_handlers/`: AWS Lambda entry points and request handling
  - `utils/`: Shared utilities for AWS services and API interactions

- `compiled/`: Contains the complete, deployable Lambda scripts generated from combining the modular components

- `tests/`: Contains automated tests for processors and Lambda handlers, with utility functionality tested

My repository at zag also contains GitHub workflows that automate:
1. Compilation of modular components into deployable Lambda scripts
2. Deployment of compiled scripts to AWS

## Prerequisites
- AWS Account with appropriate permissions
- API credentials for FirstService and SecondService
- Python 3.11+

---

## Architecture
The system architecture is built using a combination of AWS services. Below is an overview of the key services involved:

- **API Gateway**
- **EventBridge**
- **Simple Notification Service (SNS)**
- **Simple Queue Service (SQS)**
- **AWS Chatbot**
- **Lambda**
- **DynamoDB**

> **Note**: All services are deployed in the `us-east-1` AWS region. Access credentials and sensitive information are securely stored and managed using AWS Systems Manager Parameter Store.

---

## Components

### API Gateway
Six REST APIs are configured within AWS API Gateway to handle various webhook events from FirstService. These APIs trigger corresponding Lambda functions to process and relay data to SecondService.

1. **New_Timesheet_Entry**: Relays new timesheet entry event data to Lambda from FirstService via a webhook
2. **Timesheet_Update**: Relays timesheet update event data to Lambda from FirstService via a webhook
3. **Timesheet_Deletion**: Relays timesheet deletion event data to Lambda from FirstService via a webhook
4. **Timesheet_Backup**: Relays new timesheet entry event data to Lambda to extract from FirstService via a webhook
5. **Timesheet_Backup_Update**: Relays timesheet update event data to Lambda to extract from FirstService via a webhook
6. **Timesheet_Backup_Deletion**: Relays timesheet deletion event data to Lambda to extract from FirstService via a webhook

### EventBridge
Three EventBridge rules exist to automate syncing because SecondService does not support webhooks:

1. **SecondService_New_Projects_and_Clients**: Runs every 1 minute to sync new projects and clients from SecondService to FirstService
2. **Check_SecondService_Job_Updates**: Runs every 1 minute monitor job updates in SecondService and syncs them to FirstService
3. **DynamoDB_Cleanup_For_Timesheet_Entries**: Runs every 45 days to clean up timesheet entries in DynamoDB older than 45 days

### Lambda Functions
Nine Lambda functions orchestrate the core automation logic:

1. **Timesheet_Entry**: Processes new timesheet entries from FirstService and forwards them to SecondService
2. **Timesheet_Update**: Syncs updates made to existing timesheets from FirstService to SecondService
3. **Timesheet_Delete**: Syncs timesheet deletions from FirstService to SecondService
4. **Job_Client_Entry**: Syncs new projects and clients from SecondService to FirstService
5. **Job_Client_Update**: Handles job updates made in SecondService and ensures synchronization with FirstService
6. **Cleanup**: Removes outdated timesheet entries stored in DynamoDB to maintain data hygiene
7. **Backup_Entry**: A separate pipeline created to backup all timesheet entries made in FirstService, independent of the other automations
8. **Backup_Update**: Handles updates made to backed-up timesheet entries to ensure backup database remains up to date
9. **Backup_Delete**: Handles deletions made to backed-up timesheet entries to ensure backup database remains up to date

### DynamoDB
Four DynamoDB tables are utilized to store and manage data:

1. **Timesheet_Entries**: Captures completed timesheet entry data for handling updates and deletions to timesheets
2. **Label_Task_Mapping**: Maps label IDs from FirstService to corresponding task IDs in SecondService
3. **Job_Data**: Stores job-related data from SecondService for syncing between FirstService. This works to handle job closures and updates
4. **Timesheet_Backup**: Stores copies of all timesheet entries made in FirstService, independent of other automations to ensure no entries are lost due to errors or outages

### Simple Notification Service (SNS)
An SNS topic is configured to relay error messages and critical notifications from all Lambda functions to the designated AWS Chatbot, ensuring real-time monitoring and alerting.

### Simple Queue Service (SQS)
An SQS queue and dead-letter queue is set up to handle retry mechanisms for failed operations, ensuring that any issues do not result in data loss.

### AWS Chatbot
A single AWS Chatbot is set up to receive and forward error messages to the `aws-errors` Slack channel, enabling prompt issue detection.

---

## Error Logging
All errors are automatically captured and relayed through the AWS Chatbot integrated with Slack.

---

## FirstService API details
The FirstService API is robust and well documented. They use OAuth2 and support webhooks.

The FirstService API integrates with AWS API Gateway via six webhooks, each triggering specific Lambda functions to handle different types of events:

1. **/new-timesheet-entry**: Activated upon creation of new timesheet entries and sends event data to API Gateway
2. **/timesheet-deletion**: Activated when an existing timesheet entry is deleted, sending event data to API Gateway
3. **/timesheet-update**: Activated on updates to existing timesheet entries, sending data to API Gateway
4. **/timesheet-backup**: Activated upon creation of new timesheet entries and sends event data to API Gateway
5. **/timesheet-backups-update**: Activated on updates to existing timesheet entries, sending data to API Gateway
6. **/timesheet-backups-deletion**: Activated when an existing timesheet entry is deleted, sending event data to API Gateway

---

## SecondService API details
The SecondService API is session-based, has limited endpoints and does not support webhooks.

---

## Testing
Automated tests are written for each component using `pytest`, `moto`, and `unittest.mock`. Tests cover all Lambda functions, API integrations, and data synchronization logic.

---
