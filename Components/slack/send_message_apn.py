'''Send Slack DM notifying of new APN Opportunity'''

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
APN_EMAIL = env.get('APN_EMAIL')
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
    '''Fetch and return the Slack API token'''
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=credential_path, WithDecryption=True)
    return parameter['Parameter']['Value']


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


def build_message_attributes(stage):
    '''Construct message attributes based on pipedrive stage'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'slack'
        },
        'action': {
            'DataType': 'String',
            'StringValue': 'send_message_apn'
        },
        'stage': {
            'DataType': 'String',
            'StringValue': stage
        }
    }
    return message_attributes


def send_slack_message(token, slack_id, message):
    ''' Send slack message to user'''
    try:
        client = WebClient(token=token)
        resp = client.im_open(user=slack_id)
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

    channel = resp.get('channel')

    try:
        resp = client.chat_postMessage(channel=channel['id'], text=message)
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


def get_slack_id_from_email(token, email):
    '''Looks up user ID via email address'''
    try:
        client = WebClient(token=token)
        resp = client.users_lookupByEmail(email=email)
        user = resp.get('user')
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise SlackBaseError(error).with_traceback(exc_info[2])

    return user['id']


def get_slack_message(apn_link, sow_link):
    '''Returns the proper message to send to channel formatted with SOW link'''
    message = 'Please complete the APN Opportunity google sheet: {}. Link to the SOW is here: {}'.format(apn_link, sow_link)
    return message


def format_response(message):
    ''' Format the message to be returned as the response body'''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Send Message APN entry'''
    response = {'status': 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        sow_link = message['SOWLink']
        apn_link = message['APNPortalOppLink']
        pipedrive_stage = event['Records'][0]['Sns']['MessageAttributes']['stage']['Value']
        api_token = fetch_api_token(API_TOKEN_PATH)
        bot_token = fetch_api_token(BOT_TOKEN_PATH)

        # api_token needed in case channel does not exist and needs to be created
        slack_id = get_slack_id_from_email(api_token, APN_EMAIL)
        slack_message = get_slack_message(apn_link, sow_link)

        # bot_token used so the message is sent as the bot user
        send_slack_message(bot_token, slack_id, slack_message)

        # send SNS message with CustomerName, ProjectName, DealId, SOWLink, MessageStatus
        sns_message = {
            'CustomerName': message['CustomerName'],
            'ProjectName': message['ProjectName'],
            'DealId': message['DealId'],
            'SOWLink': message['SOWLink'],
            'APNPortalOppLink': message['APNPortalOppLink'],
            'MessageStatus': 'Message successfully delivered'
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
