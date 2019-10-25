from os import environ as env
import logging
import json
import sys

import boto3
from botocore.exceptions import ClientError
import requests
import pipedrive_helpers as pipedrive

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.WARNING)

STAGE = {
    1: 'lead_in',
    2: 'lead_validation',
    3: 'solution_development',
    4: 'proposal_development',
    5: 'negotiation',
    6: 'deal_closure'
}

PIPEDRIVE_SNS_TOPIC_ARN = env.get('PIPEDRIVE_SNS_TOPIC_ARN')
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


class RegressiveStageUpdateError(Exception):
    '''Error class to handle when a pipedrive stage is lower than its previous'''


def build_sns_message(deal):
    '''Construct the base sns message'''
    sns_message = {
        'CustomerName': deal['current']['org_name'],
        'ProjectName': deal['current']['title'],
        'ShortName': deal['current']['b3ac74b4fdba3bb5fe7277f0a75d17da65ee759b'],
        'EventType': deal['event'],
        'DealId': deal['current']['id']
    }
    return sns_message


def build_message_attributes(deal_event, stage, current):
    '''Construct message attributes based on deal_event'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'pipedrive'
        },
        'action': {
            'DataType': 'String',
            'StringValue': deal_event
        },
        'stage': {
            'DataType': 'String',
            'StringValue': stage
        },
        'pipeline': {
            'DataType': 'String',
            'StringValue': str(current['pipeline_id'])
        },
        'status': {
            'DataType': 'String',
            'StringValue': current['status']
        }
    }
    return message_attributes


def publish_sns_message(sns_topic_arn, message, attributes):
    '''Publish message to SNS topic'''
    print('SNS message: {}'.format(message))
    print('SNS message attributes: {}'.format(attributes))
    try:
        resp = SNS.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message),
            MessageAttributes=attributes
        )
    except ClientError as errc:
        exc_info = sys.exc_info()
        raise SnsPublishError(errc).with_traceback(exc_info[2])

    print('SNS Response: {}'.format(resp))
    return resp


def new_deal(deal, deal_event, stage, sns_topic_arn):
    '''Workflow for new deal'''
    try:
        sns_message = build_sns_message(deal)
        message_attributes = build_message_attributes(deal_event, stage, deal['current'])

        # Publish a message to Pipedrive Topic
        sns_response = publish_sns_message(sns_topic_arn,
                                           sns_message,
                                           message_attributes)
        response = {'statusCode': 200}
        response['body'] = format_response(sns_response)
    except SnsPublishError as errs:
        raise Exception(errs)
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise Exception(error).with_traceback(exc_info[2])

    return response


def updated_deal(deal, deal_event, stage):
    '''Workflow for updated deal'''
    try:
        response = {'statusCode': 200}
        # Compare differences between current and previous in deal
        current = deal['current']
        previous = deal['previous']
        diff = {}

        # If the stage hasn't changed, do not send SNS message
        if deal_event != 'added.deal':
            if current['status'] != 'won':
                if current['stage_id'] == previous['stage_id']:
                    response = {'statusCode': 202}
                    return response

        # If the stage id has decreased, do not send SNS message
        if previous:
            if current['stage_id'] < previous['stage_id']:
                raise RegressiveStageUpdateError('Current stage is less than previous stage')

            diff = {k : current[k] for k, v in set(current.items()) - set(previous.items())}

            if 'status' in diff.keys():
                if diff['status'] == 'won':
                    stage = 'deal_closure'
        # Load pipedrive credentials
        token, domain = get_pipedrive_credentials()
        # Grab all the deal fields and their information
        deal_fields = pipedrive.get_deal_fields(domain, token)
        # Build dict describing relationship between key and options
        map_items = ['Territory', 'Solution Program', 'Deal Type']

        field_map = pipedrive.build_field_map(deal_fields, map_items)
        # updates = build_update_message(current, diff, field_map)

        # Construct SNS message
        sns_message = build_sns_message(deal)
        fields = ['Territory', 'DealType', 'SolutionProgram', 'SOWLink', 'GDriveLink', 'APNPortalOppLink']
        for field in fields:
            sns_message[field] = pipedrive.get_deal_field(field_map, field, current)

        sns_message['Updates'] = build_update_message(current, diff, field_map)

        message_attributes = build_message_attributes('updated.deal', stage, current)

        # Publish a message to Pipedrive Topic
        sns_response = publish_sns_message(PIPEDRIVE_SNS_TOPIC_ARN,
                                           sns_message,
                                           message_attributes)
        response['body'] = format_response(sns_response)
    except (RegressiveStageUpdateError, SnsPublishError) as errs:
        raise Exception(errs)
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise Exception(error).with_traceback(exc_info[2])

    return response


def put_deal_db(deal):
    '''Add a new deal to the Pipedrive Deal DB'''
    table = DDB.Table('pipedrive-deals')

    try:
        response = table.put_item(
            Item={
                'customer': deal['org_name'],
                'project': deal['title'],
                'deal_id': deal['id'],
                'current_stage': deal['stage_id'],
                'pipeline_id': deal['pipeline_id'],
                'deal_status': deal['status']
            }
        )
    except ClientError as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise DynamoDBError(error).with_traceback(exc_info[2])


def update_deal_db(deal):
    '''Updates the pipedrive-deals DDB table with the current status and the current stage_id'''
    table = DDB.Table('pipedrive-deals')

    try:
        response = table.update_item(
            Key={
                'customer': deal['current']['org_name'],
                'project': deal['current']['title'],
            },
            UpdateExpression="set current_stage = :cs, deal_status = :s",
            ExpressionAttributeValues={':cs': deal['current']['stage_id'], ':s': deal['current']['status']},
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise DynamoDBError(error).with_traceback(exc_info[2])


def get_deal_db(deal):
    '''Retrieves folder_ids dict for customer project from dynamodb'''
    table = DDB.Table('pipedrive-deals')

    try:
        response = table.get_item(
            Key={
                'customer': deal['current']['org_name'],
                'project': deal['current']['title']
            }
        )
        return response['Item']
    except KeyError as errk:
        LOGGER.exception(errk)
        return []
    except ClientError as errc:
        LOGGER.exception(errc)
        exc_info = sys.exc_info()
        raise Exception(errc).with_traceback(exc_info[2])


def build_update_message(current, diff, field_map):
    msg = {}
    for key, value in diff.items():
        msg.update({key : value})
        # if key in field_map.keys():
        #     msg.update({field_map[key]['name'] : field_map[key]['{}'.format(value)]})

    return msg


def fetch_api_token(credential_path):
    ''' Fetch and return the PipeDrive API token '''
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=credential_path, WithDecryption=True)
    return parameter['Parameter']['Value']


def get_company_domain(api_token):
    url = 'https://api.pipedrive.com/v1/users/me?api_token=' + api_token

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        response = resp.json()['data']['company_domain']
    except requests.exceptions.HTTPError as errh:
        LOGGER.exception(errh)
        raise ExternalAPIFailed() from errh
    except requests.exceptions.RequestException as error:
        LOGGER.exception(error)
        raise Exception(error)

    return response

def get_pipedrive_credentials():
    token = fetch_api_token(API_TOKEN_PATH)
    domain = get_company_domain(token)
    return token, domain


def format_response(message):
    '''Format the message to be returned as the http response body'''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Webhook function entry'''
    response = {'statusCode': 200}

    print('Event received: {}'.format(event))

    try:
        deal = json.loads(event['body'])
        deal_event = deal['event']
        stage = STAGE[deal['current']['stage_id']]
        print(deal_event)

        if deal_event == 'added.deal':
            response = new_deal(deal, deal_event, 'lead_in', PIPEDRIVE_SNS_TOPIC_ARN)
            put_deal_db(deal['current'])
            if stage != 'lead_in':
                for i in range(2, deal['current']['stage_id'] + 1):
                    response = updated_deal(deal, deal_event, STAGE[i])
                update_deal_db(deal)

        elif deal_event == 'updated.deal':
            if stage == 'lead_in':
                response['body'] = format_response('No actions to perform in lead_in stage with updated.deal')
                return response
            if not get_deal_db(deal):
                response = new_deal(deal, 'added.deal', 'lead_in', PIPEDRIVE_SNS_TOPIC_ARN)
                put_deal_db(deal['current'])
                for i in range(2, deal['current']['stage_id']):
                    response = updated_deal(deal, deal_event, STAGE[i])
            response = updated_deal(deal, deal_event, stage)
            update_deal_db(deal)

    except Exception as error:
        if isinstance(error, WorthRetryingException):
            raise error

        else:
            LOGGER.exception(error)
            response['statusCode'] = 202
            message = {
                'error': {
                    'type': type(error).__name__,
                    'description': str(error),
                },
            }
            response['body'] = format_response(message)

    finally:
        return response
