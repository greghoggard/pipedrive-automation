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
BOT_TOKEN_PATH = env.get('BOT_TOKEN_PATH')
SA_EMAIL_WEST = env.get('SA_EMAIL_WEST')
SA_EMAIL_EAST = env.get('SA_EMAIL_EAST')
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
            'StringValue': 'send_message_to_sa'
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


def send_slack_message(token, slack_id, slack_message):
    ''' Get SA id and appropriate message and send slack message '''
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
        resp = client.chat_postMessage(channel=channel['id'], text=slack_message)
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


def get_slack_id_from_email(token, territory):
    '''Looks up user ID via email address'''
    if territory == 'USWEST':
        email = SA_EMAIL_WEST
    else:
        email = SA_EMAIL_EAST

    try:
        client = WebClient(token=token)
        resp = client.users_lookupByEmail(email=email)
        user = resp.get('user')
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise SlackBaseError(error).with_traceback(exc_info[2])

    return user['id']


def get_slack_message(stage, sns_message):
    '''Returns the proper message to send to Solution Architects based on
       the stage that is calling'''
       # TODO: Construct message to include the customer name and project name
    if stage == 'lead_validation':
        message = 'Please add the pre-kickoff meeting notes here: {}'.format(sns_message['CopiedFileLinks']['KickOffNotesLink'])
        return message
    elif stage == 'proposal_development':
        message = 'Please complete the Resource Request Form: {}\n'.format(sns_message['ResourceRequestLink']) +\
            'Customer: {}\nProject: {}\nSOW Link: {}'.format(sns_message['CustomerName'], sns_message['ProjectName'], sns_message['CopiedFileLinks']['SOWLink'])
        return message

    raise Exception('Invalid Stage')


def open_dm_channel(token, slack_id):
    '''Open a direct message channel with the user'''
    pass


def slack_post_message(token, channel_id):
    '''Send a message to the the direct message channel ID'''
    pass


def format_response(message):
    ''' Format the message to be returned as the response body'''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Send message to SA entry'''
    response = {'status': 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        territory = message['Territory']
        pipedrive_stage = event['Records'][0]['Sns']['MessageAttributes']['stage']['Value']
        token = fetch_api_token(BOT_TOKEN_PATH)

        slack_id = get_slack_id_from_email(token, territory)
        slack_message = get_slack_message(pipedrive_stage, message)

        send_slack_message(token, slack_id, slack_message)

        # send SNS message with CustomerName, ProjectName, MessageStatus
        sns_message = {
            'CustomerName': message['CustomerName'],
            'ProjectName': message['ProjectName'],
            'DealId': message['DealId'],
            'MessageStatus': '{} Message successfully sent to SA'.format(pipedrive_stage)
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
