'''Update slack-active-deals table with project information when a deal is won'''

from os import environ as env
import logging
import json
import sys
import datetime

import boto3
from botocore.exceptions import ClientError
from slack import WebClient

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.WARNING)


SLACK_SNS_TOPIC_ARN = env.get('SLACK_SNS_TOPIC_ARN')
BOT_TOKEN_PATH = env.get('BOT_TOKEN_PATH')
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


def fetch_api_token(credential_path):
    '''Fetch and return the Slack API token'''
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=credential_path, WithDecryption=True)
    return parameter['Parameter']['Value']


def build_message_attributes(action, attributes):
    '''Construct message attributes based on pipedrive stage'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'slack'
        },
        'action': {
            'DataType': 'String',
            'StringValue': action
        },
        'stage': {
            'DataType': 'String',
            'StringValue': attributes['stage']['Value']
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


def update_project(message):
    '''Updates the slack-customers DDB table with the current status and weekly status report and engagement data links'''
    table = DDB.Table('slack-customers')

    try:
        response = table.update_item(
            Key={
                'customer': message['CustomerName'],
                'project': message['ProjectName']
            },
            UpdateExpression="set current_stage = :cs, deal_status = :s, engagement_data_link = :ed, weekly_status_report = :wr",
            ExpressionAttributeValues={
                ':cs': 'deal_closure',
                ':s': 'won',
                ':ed': message['CopiedFileLinks']['EngagementDataPointsLink'],
                ':wr': {get_friday_date() : message['CopiedFileLinks']['WeeklyStatusReportLink']}
            },
            ReturnValues="ALL_NEW"
        )

        return response['Attributes']
    except ClientError as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise DynamoDBError(error).with_traceback(exc_info[2])


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


def get_slack_message(engagement_report_link):
    '''Returns a formatted message'''
    message = '@here Engagement Lead, please complete the Engagement Data Report\n' +\
        'to capture the customer environment prior to us implementing solutions\n' +\
        'Engagement Data Report: {}'.format(engagement_report_link)
    return message


def format_response(message):
    ''' Format the message to be returned as the response body'''
    message = {'message': message}
    return json.dumps(message)


def get_friday_date():
    day = datetime.date.today()
    while day.weekday() != 4:
        day += datetime.timedelta(1)

    return day.strftime('%m-%d-%Y')


def lambda_handler(event, context):
    '''Send Message APN entry'''
    response = {'status': 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        bot_token = fetch_api_token(BOT_TOKEN_PATH)

        # Update project status  and doc links
        project = update_project(message)

        # Send slack message to project channel
        slack_message = get_slack_message(message['CopiedFileLinks']['WeeklyStatusReportLink'])
        send_slack_message(bot_token, project['channels']['project_id'], slack_message)

        # Publish a message to Slack Topic
        sns_message = {
            'CustomerName': message['CustomerName'],
            'ProjectName': message['ProjectName'],
            'DealId': message['DealId']
        }

        message_attributes = build_message_attributes('deal_won', event['Records'][0]['Sns']['MessageAttributes'])
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
