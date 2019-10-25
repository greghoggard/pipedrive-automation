'''Create Slack Channel for CustomerName and ProjectName'''

from os import environ as env
import logging
import json
import sys

import boto3
from botocore.exceptions import ClientError
from slack import WebClient

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.WARNING)


SLACK_SNS_TOPIC_ARN = env.get('SLACK_SNS_TOPIC_ARN')
API_TOKEN_PATH = env.get('API_TOKEN_PATH')
SNS = boto3.client('sns')
DDB = boto3.resource('dynamodb', region_name='us-east-1')


class WorthRetryingException(Exception):
    '''Base error class'''


class ExternalAPIFailed(WorthRetryingException):
    '''External API error class'''


class TemporaryGlitch(WorthRetryingException):
    '''Idempotent Glitch error class'''


class DynamoDBError(WorthRetryingException):
    '''DynamoDB error'''


class SnsPublishError(Exception):
    '''SNS publish error'''


class SlackBaseError(Exception):
    '''Base Slack error class'''


def build_message_attributes():
    '''Construct message attributes'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'slack'
        },
        'action': {
            'DataType': 'String',
            'StringValue': 'create_channel'
        },
        'stage': {
            'DataType': 'String',
            'StringValue': 'lead_in'
        }
    }
    return message_attributes


def publish_sns_message(sns_topic_arn, message, attributes):
    '''Publish message to SNS topic'''
    print('SNS message: {}'.format(message))
    try:
        resp = SNS.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message),
            MessageAttributes=attributes
        )
    except ClientError as errc:
        LOGGER.exception(errc)
        exc_info = sys.exc_info()
        raise SnsPublishError(errc).with_traceback(exc_info[2])

    print('SNS Response: {}'.format(resp))
    return resp

def fetch_api_token(credential_path):
    '''Fetch and return the Slack API token'''
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=credential_path, WithDecryption=True)
    return parameter['Parameter']['Value']


def create_channel(token, name):
    '''Creates Slack Channel with the provided name'''
    print('Creating Slack Channel: {0}'.format(name))

    try:
        client = WebClient(token=token)
        resp = client.channels_create(name=name)
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise SlackBaseError(error).with_traceback(exc_info[2])

    if resp.get('ok') is False:
        errors = {
            'error': resp['error'],
            'detail': resp['detail']
        }
        message = {'status': 500, 'channel_name': '', 'error': errors}
        return message

    channel = resp.get('channel')
    message = {'status': 200, 'channel_name': channel['name'], 'channel_id': channel['id'], 'error': None}
    return message


def update_deal_db(msg, msg_attr, slack_channels):
    '''Updates the pipedrive-deals DDB table with the current status and the current stage_id'''
    table = DDB.Table('slack-customers')

    try:
        response = table.put_item(
            Item={
                'customer': msg['CustomerName'],
                'project': msg['ProjectName'],
                'deal_id': msg['DealId'],
                'current_stage': msg_attr['stage']['Value'],
                'pipeline_id': msg_attr['pipeline']['Value'],
                'deal_status': msg_attr['status']['Value'],
                'channels': {
                    'customer_id': slack_channels['CustomerChannel']['id'],
                    'project_id': slack_channels['ProjectChannel']['id']
                }
            }
        )
    except ClientError as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise DynamoDBError(error).with_traceback(exc_info[2])


def sanitize_slack_channel_name(channel_name):
    '''Cleanup channel name'''
    clean_channel = channel_name.replace(' ', '-').lower()
    clean_channel = ''.join(e for e in clean_channel if e.isalnum() or e == '-')
    return clean_channel


def check_slack_channel_exists(token, channel_name):
    '''Returns True/False based on existence of slack channel'''
    client = WebClient(token=token)

    resp = client.channels_list()
    for channel in resp.get('channels'):
        this_channel = channel.get('name')
        if channel_name == this_channel:
            return channel.get('id')

    return []


def format_response(message):
    ''' Format the message to be returned as the response body '''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Slack Create Channel entry'''
    response = {'status': 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        msg_attr = event['Records'][0]['Sns']['MessageAttributes']
        token = fetch_api_token(API_TOKEN_PATH)

        slack_channels = {}

        short_name = message['ShortName']
        if short_name is None:
            # If ShortName is not present, grab first 3 letters of customer name
            short_name = message['CustomerName'][:3]

        cust_channel_name = sanitize_slack_channel_name(short_name)
        channel_id = check_slack_channel_exists(token, cust_channel_name)
        if not channel_id:
            customer_resp = create_channel(token, cust_channel_name)
            slack_channels['CustomerChannel'] = {
                'name': customer_resp['channel_name'],
                'id': customer_resp['channel_id']
            }
        else:
            slack_channels['CustomerChannel'] = {
                'name': cust_channel_name,
                'id': channel_id
            }

        # Project channel name is combination of CustomerName and ProjectName
        project_channel_name = sanitize_slack_channel_name(short_name + '-' + message['ProjectName'])
        channel_id = check_slack_channel_exists(token, project_channel_name)
        if not channel_id:
            project_resp = create_channel(token, project_channel_name)
            slack_channels['ProjectChannel'] = {
                'name': project_resp['channel_name'],
                'id': project_resp['channel_id']
            }
        else:
            slack_channels['ProjectChannel'] = {
                'name': project_channel_name,
                'id': channel_id
            }

        # Update slack-customers table with customer and project info and channel details
        update_deal_db(message, msg_attr, slack_channels)

        # Build SNS message with CustomerName, ProjectName, ShortName, DealId, SlackChannel Names
        sns_message = {
            'CustomerName': message['CustomerName'],
            'ProjectName': message['ProjectName'],
            'ShortName': short_name,
            'DealId': message['DealId'],
            'SlackChannels': {
                'CustomerChannel': slack_channels['CustomerChannel'],
                'ProjectChannel': slack_channels['ProjectChannel']
            }
        }

        # Publish a message to Slack Topic
        message_attributes = build_message_attributes()
        sns_response = publish_sns_message(SLACK_SNS_TOPIC_ARN,
                                           sns_message,
                                           message_attributes)
        response['body'] = format_response(sns_response)

    except Exception as error:
        if isinstance(error, WorthRetryingException):
            raise error

        else:
            LOGGER.exception(error)
            response['statusCode'] = 500
            message = {
                'error': {
                    'type': type(error).__name__,
                    'description': str(error),
                },
            }
            response['body'] = format_response(message)

    return response
