'''Send Slack message to #engagement-review channel'''

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
BOT_TOKEN_PATH = env.get('BOT_TOKEN_PATH')
SNS = boto3.client('sns')


class WorthRetryingException(Exception):
    '''Base error class'''


class ExternalAPIFailed(WorthRetryingException):
    '''External API error class'''


class TemporaryGlitch(WorthRetryingException):
    '''Idempotent Glitch error class'''


class SnsPublishError(Exception):
    '''SNS publish error'''


class SlackBaseError(Exception):
    '''Base Slack error class'''


def fetch_api_token(credential_path):
    ''' Fetch and return the Slack API token '''
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=credential_path, WithDecryption=True)
    return parameter['Parameter']['Value']


def build_message_attributes(stage):
    '''Construct message attributes based on pipedrive stage'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'slack'
        },
        'action': {
            'DataType': 'String',
            'StringValue': 'send_message_engagement_review'
        },
        'stage': {
            'DataType': 'String',
            'StringValue': stage
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


def send_slack_message(token, channel_id, message):
    ''' Send slack message to channel'''
    try:
        client = WebClient(token=token)
        resp = client.chat_postMessage(channel=channel_id, text=message)
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise SlackBaseError(error).with_traceback(exc_info[2])

    if resp.get('ok') is False:
        errors = {
            'error': resp['error'],
            'detail': resp['detail']
        }
        LOGGER.exception(errors)
        raise Exception(errors)


def get_channel_id(token, channel_name):
    '''Returns channel ID of given channel name. If channel does not exist,
        creates the channel and returns the ID'''
    try:
        client = WebClient(token=token)
        resp = client.channels_list()
        for channel in resp.get('channels'):
            this_channel = channel.get('name')
            if channel_name == this_channel:
                return channel.get('id')
        channel_id = create_channel(token, channel_name)
        return channel_id
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise SlackBaseError(error).with_traceback(exc_info[2])


def get_slack_message(sow_link):
    '''Returns the proper message to send to channel formatted with SOW link'''
    message = '@here Please review the included SOW and provide feedback with any areas of concern within 24 hours: {}'.format(sow_link)
    return message


def create_channel(token, name):
    '''Create slack channel if non-existent'''
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
        LOGGER.exception(errors)
        raise Exception(message)

    channel = resp.get('channel')
    return channel['id']


def format_response(message):
    ''' Format the message to be returned as the response body'''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Send message engagement review entry'''
    response = {'status': 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        sow_link = message['SOWLink']
        pipedrive_stage = event['Records'][0]['Sns']['MessageAttributes']['stage']['Value']
        api_token = fetch_api_token(API_TOKEN_PATH)
        bot_token = fetch_api_token(BOT_TOKEN_PATH)

        # api_token needed in case channel does not exist and needs to be created
        channel_id = get_channel_id(api_token, 'sales-engagement-review')
        slack_message = get_slack_message(sow_link)

        # bot_token used so the message is sent as the bot user
        send_slack_message(bot_token, channel_id, slack_message)

        # send SNS message with CustomerName, ProjectName, DealId, SOWLink, MessageStatus
        sns_message = {
            'CustomerName': message['CustomerName'],
            'ProjectName': message['ProjectName'],
            'DealId': message['DealId'],
            'SOWLink': message['SOWLink'],
            'MessageStatus': 'Message successfully delivered to #sales-engagement-review'
        }

        # Publish a message to Slack Topic
        message_attributes = build_message_attributes(pipedrive_stage)
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

    finally:
        return response
