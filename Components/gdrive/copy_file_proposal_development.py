'''Copies document templates into project folders for the Lead Validation phase'''

from os import environ as env
from operator import eq, ne
import logging
import json
import sys
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from pydrive.auth import GoogleAuth, AuthError
from pydrive.drive import GoogleDrive
from pydrive.files import ApiRequestError, FileNotUploadedError
from pydrive.settings import InvalidConfigError

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.WARNING)

GDRIVE_SNS_TOPIC_ARN = env.get('GDRIVE_SNS_TOPIC_ARN')
GDRIVE_PARENT_FOLDER_ID = env.get('GDRIVE_PARENT_FOLDER_ID')
GDRIVE_DOC_TEMPLATE_FOLDER_ID = env.get('GDRIVE_DOC_TEMPLATE_FOLDER_ID')
RESOURCE_REQUEST_LINK = env.get('RESOURCE_REQUEST_LINK')
SNS = boto3.client('sns')

class WorthRetryingException(Exception):
    '''Base error class for exceptions worth retrying'''


class GDriveAuthError(WorthRetryingException):
    '''General authentication error'''
    # Worth retrying until we discover which errors are impossible to rectify


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
        LOGGER.exception(erra)
        exc_info = sys.exc_info()
        raise GDriveAuthError(erra).with_traceback(exc_info[2])
    except InvalidConfigError as errc:
        LOGGER.exception(errc)
        exc_info = sys.exc_info()
        raise GDriveBaseError(errc).with_traceback(exc_info[2])

    return GoogleDrive(gauth)


def get_resource_request_link(credential_path):
    """ Fetch and return the Resource Request Form Link """
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=credential_path, WithDecryption=True)
    return parameter['Parameter']['Value']


def build_message_attributes():
    '''Construct message attributes'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'gdrive'
        },
        'action': {
            'DataType': 'String',
            'StringValue': 'copy_files'
        },
        'stage': {
            'DataType': 'String',
            'StringValue': 'proposal_development'
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


def copy_file(drive, source_id, dest_title, parent_id):
    '''Copy an existing file'''
    copied_file = {
        'title': dest_title,
        'parents': [
            {
                'id': parent_id
            }
        ]
        }
    try:
        file_data = drive.auth.service.files().copy(
            fileId=source_id, body=copied_file).execute()
        return drive.CreateFile({'id': file_data['id']})
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise Exception(error).with_traceback(exc_info[2])


def get_docs_to_copy(stage_name, sow_folder_id, customer, project):
    docs = {
        "proposal_development": {
            "{}_Stelligent_AWS_{}_{}_SOW".format(customer, project, datetime.today().strftime('%m%d%Y')): {
                "id": "19UCMq12ErhGikyuARQqR6_dYLkpXEwWoTgwmHaXxK8g",
                "dest": sow_folder_id,
                "field_name": 'SOWLink'
            }
        }
    }

    try:
        return docs[stage_name]
    except KeyError as errk:
        LOGGER.exception(errk)
        raise GDriveBaseError(errk)


def copy_files_from_doclist(drive, stage_doc_list):
    '''Iterate over doc list and copy each file to destination folder'''
    copied_file_links = {}
    errors = []
    for (title, info) in stage_doc_list.items():
        try:
            result = copy_file(drive, info['id'], title, info['dest'])
            copied_file_links.update({info['field_name'] : result['alternateLink']})
        except Exception as error:
            LOGGER.exception(error)
            errors.append(error)
    if errors:
        print('Errors received: {}'.format(errors))

    return copied_file_links


def get_project_sub_folder_id(drive, parent_folder_id, customer_name, project_name, folder_name):
    '''Return sales project folder id by iterating through root folder
       down to customer folder then Sales folder and finally project folder'''
    folder_list = list_file_object(drive, parent_folder_id, directory_only=True)
    match = [x for x in folder_list if x['title'] == customer_name]

    if match:
        folder_id = match[0]['id']
        customer_folder_list = list_file_object(drive, folder_id, directory_only=True)
        sales_match = [x for x in customer_folder_list if x['title'] == '_SALES']
        if sales_match:
            sales_folder_id = sales_match[0]['id']
            sales_folder_list = list_file_object(drive, sales_folder_id, directory_only=True)
            project_match = [x for x in sales_folder_list if x['title'] == 'Project Name: {}'.format(project_name)]
            if project_match:
                project_folder_id = project_match[0]['id']
                project_folder_list = list_file_object(drive, project_folder_id, directory_only=True)
                meeting_note_match = [x for x in project_folder_list if x['title'] == folder_name]
                return meeting_note_match[0]['id']
    else:
        raise GDriveBaseError('{} folder does not exist'.format(folder_name))


def list_file_object(drive, folder_id, directory_only=False):
    '''Iterates over a folder and returns list of all child objects'''
    _q = {'q': "'{}' in parents and trashed=false".format(folder_id)}
    file_object_list = drive.ListFile(_q).GetList()
    op = {True: eq, False: ne}[directory_only]
    file_objects = [
        x for x in file_object_list
        if op(x['mimeType'], 'application/vnd.google-apps.folder')
    ]
    return [{"id": fld["id"], "title": fld["title"]} for fld in file_objects]


def format_response(message):
    ''' Format the message to be returned as the response body'''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    response = {"status": 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        pipedrive_stage = event['Records'][0]['Sns']['MessageAttributes']['stage']['Value']
        customer_name = message['CustomerName']
        project_name = message['ProjectName']

        # Initialize GDrive authentication
        drive = init_auth()

        project_sow_folder_id = get_project_sub_folder_id(drive, GDRIVE_PARENT_FOLDER_ID, customer_name, project_name, 'SOW')

        # Based on pipedrive stage, grab the docs that need to be copied
        doc_list = get_docs_to_copy(pipedrive_stage, project_sow_folder_id, customer_name, project_name)
        copied_file_links = copy_files_from_doclist(drive, doc_list)

        # Publish a message to GDrive Topic
        sns_message = {
            "CustomerName": customer_name,
            "ProjectName": project_name,
            "DealId": message['DealId'],
            "Territory": message['Territory'],
            "CopiedFileLinks": copied_file_links,
            "ResourceRequestLink": get_resource_request_link(RESOURCE_REQUEST_LINK) # Move to Slack component
        }
        message_attributes = build_message_attributes()
        sns_response = publish_sns_message(GDRIVE_SNS_TOPIC_ARN,
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