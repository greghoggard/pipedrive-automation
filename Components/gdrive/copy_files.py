'''Copies document templates into project folders for the Lead In phase'''

from os import environ as env
from operator import eq, ne
import logging
import json
import sys
from datetime import datetime

import fiscalyear
from fiscalyear import FiscalDateTime
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
SNS = boto3.client('sns')
DDB = boto3.resource('dynamodb', region_name='us-east-1')

fiscalyear.START_MONTH = 4
FISCAL_YEAR = FiscalDateTime.now()

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


class GDriveFolderNotFoundError(WorthRetryingException):
    '''GDrive folder missing error'''


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


def build_sns_message(message, copied_file_links, folder_ids=None):
    '''Construct SNS message and include info about the fields that were updated'''
    sns_message = {
        "CustomerName": message['CustomerName'],
        "ProjectName": message['ProjectName'],
        "DealId": message['DealId'],
        "Territory": message['Territory'],
        "FolderIds": folder_ids,
        "CopiedFileLinks": copied_file_links
    }
    return sns_message


def build_message_attributes(action, stage):
    '''Construct message attributes'''
    message_attributes = {
        'component': {
            'DataType': 'String',
            'StringValue': 'gdrive'
        },
        'action': {
            'DataType': 'String',
            'StringValue': action
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
    except HttpError as errh:
        raise errh
    except Exception as error:
        exc_info = sys.exc_info()
        raise Exception(error).with_traceback(exc_info[2])


def get_doc_template_ids(stage):
    '''Retrieve Doc Template IDs for this stage from DynamoDB'''
    doc_templates = {}
    table = DDB.Table('gdrive-doc-templates')

    try:
        response = table.query(
            KeyConditionExpression=Key('stage').eq(stage)
        )
    except ClientError as errc:
        exc_info = sys.exc_info()
        raise Exception(errc).with_traceback(exc_info[2])

    for i in response['Items']:
        doc_templates.update({i['tag'] : i['id']})
    return doc_templates


def get_folder_ids(message):
    '''Retrieves folder_ids dict for customer project from dynamodb'''
    table = DDB.Table('gdrive-customers')

    try:
        response = table.get_item(
            Key={
                'customer': message['CustomerName'],
                'project': message['ProjectName']
            },
            ProjectionExpression='folder_ids'
        )
    except ClientError as errc:
        LOGGER.exception(errc)
        exc_info = sys.exc_info()
        raise Exception(errc).with_traceback(exc_info[2])

    try:
        return response['Item']['folder_ids']
    except KeyError as errk:
        LOGGER.exception(errk)
        sns_message = build_sns_message(message, {})
        message_attributes = build_message_attributes('folder_missing', 'error')
        publish_sns_message(GDRIVE_SNS_TOPIC_ARN, sns_message, message_attributes)
        raise GDriveFolderNotFoundError('Gdrive Folders missing for {} - {}'.format(message['CustomerName'], message['ProjectName']))


def get_docs_to_copy(message, doc_templates, stage_name, folder_ids):
    '''Returns a formatted dict of documents that need to be copied'''
    docs = {
        'lead_in': {
            '{}_Account_Plan_Q{}_{}'.format(message['CustomerName'], FISCAL_YEAR.quarter, datetime.today().strftime('%Y')): {
                'tag': 'AccountPlan',
                'dest': folder_ids['AccountFolder']['RootId'],
                'field_name': 'AccountPlanLink'
            },
            '{}_{}_Risk Log'.format(message['CustomerName'], message['ProjectName']): {
                'tag': 'RiskLog',
                'dest': folder_ids['SalesFolder']['ProjectId'],
                'field_name': 'RiskLogLink'
            },
            'Add New APN Opportunity': {
                'tag': 'APNPortalOpp',
                'dest': folder_ids['SalesFolder']['SubFolders']['APN Portal Admin'],
                'field_name': 'APNPortalOppLink'
            }
        },
        "lead_validation": {
            "Pre-KickOff Project Notes": {
                "tag": 'KickOffNotes',
                "dest": folder_ids['SalesFolder']['SubFolders']['Meeting_Notes'],
                "field_name": "KickOffNotesLink"
            }
        },
        'deal_closure': {
            '{}-{}_Weekly_Status_Report_{}'.format(message['CustomerName'], message['ProjectName'], datetime.today().strftime('%m-%d-%Y')): {
                'tag': 'WeeklyStatusReport',
                'dest': folder_ids['DeliveryFolder']['SubFolders']['Weekly_Action_Reports'],
                'field_name': 'WeeklyStatusReportLink'
            },
            'Engagement_Data': {
                'tag': 'EngagementDataPoints',
                'dest': folder_ids['DeliveryFolder']['SubFolders']['Engagement_Data_Reports'],
                'field_name': 'EngagementDataPointsLink'
            }
        }
    }

    try:
        for (title, info) in docs[stage_name].items():
            info['id'] = doc_templates[info['tag']]
        return docs[stage_name]
    except KeyError as errk:
        LOGGER.exception(errk)
        raise GDriveBaseError(errk)


def copy_files_from_doclist(drive, stage_doc_list, message):
    '''Iterate over doc list and copy each file to destination folder'''
    copied_file_links = {}
    errors = []
    for (title, info) in stage_doc_list.items():
        match = check_file_exists(drive, info['dest'], title)
        if not match:
            try:
                result = copy_file(drive, info['id'], title, info['dest'])
                copied_file_links.update({info['field_name'] : result['alternateLink']})
            except HttpError as errh:
                if errh.resp.status == 404:
                    sns_message = build_sns_message(message, copied_file_links)
                    message_attributes = build_message_attributes('folder_missing', 'error')
                    publish_sns_message(GDRIVE_SNS_TOPIC_ARN, sns_message, message_attributes)
                    raise GDriveFolderNotFoundError(errh)
            except Exception as error:
                LOGGER.exception(error)
                errors.append(error)
        else:
            file_object = drive.CreateFile({'id': match[0]['id']})
            copied_file_links.update({info['field_name'] : file_object['alternateLink']})
    if errors:
        print('Errors received: {}'.format(errors))

    return copied_file_links


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


def check_file_exists(drive, parent_folder_id, title):
    '''Check if a folder with the given title exists within the parent folder'''
    folder_list = list_file_object(
        drive,
        parent_folder_id
    )
    match = [x for x in folder_list if x['title'] == title]
    return match


def format_response(message):
    ''' Format the message to be returned as the response body '''
    message = {'message': message}
    return json.dumps(message)


def lambda_handler(event, context):
    '''Copy files Lead In entry'''
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    response = {"status": 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        pipedrive_stage = event['Records'][0]['Sns']['MessageAttributes']['stage']['Value']

        if pipedrive_stage == 'lead_in':
            folder_ids = message['FolderIds']
        else:
            # Retrieve folder ids from dynamodb
            folder_ids = get_folder_ids(message)

        # Initialize GDrive authentication
        drive = init_auth()

        # Based on pipedrive stage, grab the docs that need to be copied
        doc_templates = get_doc_template_ids(pipedrive_stage)
        doc_list = get_docs_to_copy(message, doc_templates, pipedrive_stage, folder_ids)
        copied_file_links = copy_files_from_doclist(drive, doc_list, message)

        # Publish a message to Gdrive Topic
        sns_message = build_sns_message(message, copied_file_links, folder_ids)
        message_attributes = build_message_attributes('copy_files', pipedrive_stage)
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
