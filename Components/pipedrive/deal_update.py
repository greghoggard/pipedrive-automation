'''Update Pipedrive Deal Fields'''

from os import environ as env
import logging
import json
import sys

import boto3
from botocore.exceptions import ClientError
import requests

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.WARNING)

PIPEDRIVE_API_TOKEN_PATH = env.get('API_TOKEN_PATH')
PIPEDRIVE_SNS_TOPIC_ARN = env.get('PIPEDRIVE_SNS_TOPIC_ARN')
SNS = boto3.client('sns')


class WorthRetryingException(Exception):
    '''Base error class'''


class ExternalAPIFailed(WorthRetryingException):
    '''External API error class'''


class TemporaryGlitch(WorthRetryingException):
    '''Idempotent Glitch error class'''


class SnsPublishError(Exception):
    '''SNS publish error'''


class PipedriveRequestError(WorthRetryingException):
    '''Pipedrive Request error'''


def fetch_api_token(credential_path):
    """ Fetch and return the PipeDrive API token """
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=credential_path, WithDecryption=True)
    return parameter['Parameter']['Value']


def get_company_domain(api_token):
    '''Pipedrive call using the api token to return the company domain'''
    url = 'https://api.pipedrive.com/v1/users/me?api_token=' + api_token

    resp = requests.get(url)
    resp.raise_for_status()
    response = resp.json()['data']['company_domain']

    return response


def build_sns_message(message, fields_to_update):
    '''Construct SNS message and include info about the fields that were updated'''
    sns_message = {
        'CustomerName': message['CustomerName'],
        'ProjectName': message['ProjectName'],
        'DealId': message['DealId'],
        'DealFieldLinksUpdated': {}
    }
    if fields_to_update is not None:
        for (key, value) in fields_to_update.items():
            sns_message['DealFieldLinksUpdated'].update({key : value})
    return sns_message


def build_message_attributes(stage):
    '''Construct message attributes based on pipedrive stage'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'pipedrive'
        },
        'action': {
            'DataType': 'String',
            'StringValue': 'update_deal_fields'
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
        exc_info = sys.exc_info()
        raise SnsPublishError(errc).with_traceback(exc_info[2])

    print('SNS Response: {}'.format(resp))
    return resp


def get_pipedrive_credentials():
    '''Retrieve Pipedrive credentials'''
    try:
        token = fetch_api_token(PIPEDRIVE_API_TOKEN_PATH)
        domain = get_company_domain(token)
    except (ClientError, requests.exceptions.HTTPError) as errc:
        LOGGER.exception(errc)
        exc_info = sys.exc_info()
        raise ExternalAPIFailed(errc).with_traceback(exc_info[2])
    except requests.exceptions.RequestException as errr:
        LOGGER.exception(errr)
        exc_info = sys.exc_info()
        raise PipedriveRequestError(errr).with_traceback(exc_info[2])

    return token, domain

def get_deal_fields(domain, token, fields_to_update):
    '''Retrieve all Deal Fields from Pipedrive and return a formatted dict
       with only the Deal Field that are going to be updated'''
    url = 'https://{}.pipedrive.com/v1/dealFields:(key,name)?start=0&api_token={}'.format(domain, token)
    formatted_fields = {}

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        fields = resp.json()['data']
    except (ClientError, requests.exceptions.HTTPError) as errc:
        LOGGER.exception(errc)
        exc_info = sys.exc_info()
        raise ExternalAPIFailed(errc).with_traceback(exc_info[2])
    except requests.exceptions.RequestException as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise PipedriveRequestError(error).with_traceback(exc_info[2])

    for field in fields:
        for (key, value) in fields_to_update.items():
            if field['name'].replace(' ', '') == key:
                formatted_fields.update({key : {field['key'] : value}})

    return formatted_fields


def update_deal_field(domain, token, deal_id, field_name, field_value):
    '''PUT call to Pipedrive to update Deal Field with new value'''
    url = 'https://{}.pipedrive.com/v1/deals/{}?api_token={}'.format(domain, deal_id, token)

    data = {}
    data[field_name] = field_value

    try:
        resp = requests.put(
            url,
            data=data
        )
        resp.raise_for_status()
        result = resp.json()
        if result['data'] is None:
            raise ExternalAPIFailed('Updating {} field failed'.format(field_name))
        elif result['data']['id'] is not None:
            return
        else:
            raise Exception("Updating {} field was unsuccessful".format(field_name))
    except (ClientError, requests.exceptions.HTTPError) as errc:
        LOGGER.exception(errc)
        exc_info = sys.exc_info()
        raise ExternalAPIFailed(errc).with_traceback(exc_info[2])
    except requests.exceptions.RequestException as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise PipedriveRequestError(error).with_traceback(exc_info[2])


def format_response(message):
    '''Format the message to be returned as the http response body'''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Pipedrive Deal Update function entry'''
    response = {'statusCode': 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        pipedrive_stage = event['Records'][0]['Sns']['MessageAttributes']['stage']['Value']
        pipedrive_action = event['Records'][0]['Sns']['MessageAttributes']['action']['Value']

        token, domain = get_pipedrive_credentials()
        if pipedrive_action == 'create_folders':
            fields_to_update = get_deal_fields(domain, token, message['DealFieldLinks'])
        else:
            fields_to_update = get_deal_fields(domain, token, message['CopiedFileLinks'])

        if fields_to_update is not None:
            for (name, links) in fields_to_update.items():
                for (key, value) in links.items():
                    update_deal_field(domain, token, message['DealId'], key, value)

        # Construct SNS message
        sns_message = build_sns_message(message, fields_to_update)
        message_attributes = build_message_attributes(pipedrive_stage)

        # Publish message to Pipedrive SNS Topic
        sns_response = publish_sns_message(PIPEDRIVE_SNS_TOPIC_ARN,
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
