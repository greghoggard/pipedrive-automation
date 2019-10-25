'''Retrieves information about Doc Templates from Gdrive and updates dynamodb'''

from os import environ as env
from operator import eq, ne
import logging
import json
import sys

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from googleapiclient.errors import HttpError
from pydrive.auth import GoogleAuth, AuthError
from pydrive.drive import GoogleDrive
from pydrive.files import ApiRequestError, FileNotUploadedError
from pydrive.settings import InvalidConfigError

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.WARNING)

GDRIVE_SNS_TOPIC_ARN = env.get('GDRIVE_SNS_TOPIC_ARN')
GDRIVE_DOC_TEMPLATE_FOLDER_ID = env.get('GDRIVE_DOC_TEMPLATE_FOLDER_ID')
SNS = boto3.client('sns')
DDB = boto3.resource('dynamodb', region_name='us-east-1')


class WorthRetryingException(Exception):
    '''Base error class for exceptions worth retrying'''


class GDriveAuthError(WorthRetryingException):
    '''General authentication error'''


class TemporaryGlitch(WorthRetryingException):
    '''Idempotent Glitch error class'''


class SnsPublishError(Exception):
    '''SNS publish error'''


class GDriveBaseError(Exception):
    '''Base GDrive error'''


def init_auth(settings_file='settings.yaml'):
    '''Initialize GoogleDrive auth object'''
    try:
        gauth = GoogleAuth(
            settings_file=settings_file
        )
        gauth.ServiceAuth()
    except AuthError as erra:
        exc_info = sys.exc_info()
        raise GDriveAuthError(erra).with_traceback(exc_info[2])
    except InvalidConfigError as errc:
        exc_info = sys.exc_info()
        raise GDriveBaseError(errc).with_traceback(exc_info[2])

    return GoogleDrive(gauth)


def build_sns_message():
    '''Construct SNS message and include info about the fields that were updated'''
    # TODO: Accept errors as param and list any errors that occur
    sns_message = {
        'Status': 'Update Successful'
    }
    return sns_message


def build_message_attributes():
    '''Construct message attributes'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'gdrive'
        },
        'action': {
            'DataType': 'String',
            'StringValue': 'update_db'
        },
        'stage': {
            'DataType': 'String',
            'StringValue': 'doc_templates'
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


def list_file_object(drive, folder_id, directory_only=False):
    '''Iterates over a folder and returns list of all child objects'''
    _q = {'q': "'{}' in parents and trashed=false".format(folder_id)}
    file_object_list = drive.ListFile(_q).GetList()
    op = {True: eq, False: ne}[directory_only]
    file_objects = [
        x for x in file_object_list
        if op(x['mimeType'], 'application/vnd.google-apps.folder')
    ]
    return [{'id': fld['id'], 'title': fld['title']} for fld in file_objects]


def get_properties(drive, file_id):
    '''Retrieves "stage" and "tag" GdriveFile properties'''
    prop = {
        'stage': 'none',
        'tag': 'untagged'
    }
    props = drive.auth.service.properties().list(fileId=file_id).execute()
    if props['items']:
        for prop in props['items']:
            try:
                tag_prop = drive.auth.service.properties().get(fileId=file_id, propertyKey='tag').execute()
                prop['tag'] = tag_prop['value']
            except HttpError as errh:
                pass
            try:
                stage_prop = drive.auth.service.properties().get(fileId=file_id, propertyKey='stage').execute()
                prop['stage'] = stage_prop['value']
            except HttpError as errh:
                pass

    return prop


def format_response(message):
    ''' Format the message to be returned as the response body '''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Gdrive Update Doc Templates entry'''

    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    response = {"status": 200}

    print('Event received: {}'.format(event))

    table = DDB.Table('gdrive-doc-templates')

    drive = init_auth()
    doc_list = list_file_object(drive, GDRIVE_DOC_TEMPLATE_FOLDER_ID)

    try:
        for doc in doc_list:
            # Retrieve stage and tag properties if they are present
            prop = get_properties(drive, doc['id'])

            # Update gdrive-doc-templates table
            table.put_item(
                Item={
                    'stage': prop['stage'],
                    'tag': prop['tag'],
                    'title': doc['title'],
                    'id': doc['id']
                }
            )

        # Publish a message to Gdrive Topic
        sns_message = build_sns_message()
        message_attributes = build_message_attributes()
        sns_response = publish_sns_message(GDRIVE_SNS_TOPIC_ARN,
                                           sns_message,
                                           message_attributes)

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
