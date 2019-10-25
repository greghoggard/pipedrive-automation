'''Create GDrive folder structure for a new customer and/or project'''

from os import environ as env
from operator import eq, ne
import logging
import json
import sys

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
SNS = boto3.client('sns')
DDB = boto3.resource('dynamodb', region_name='us-east-1')


class WorthRetryingException(Exception):
    '''Base error class for exceptions worth retrying'''


class ExternalAPIFailed(WorthRetryingException):
    '''External API error class'''


class SnsPublishError(Exception):
    '''SNS publish error'''


class DynamoDBError(WorthRetryingException):
    '''DynamoDB error'''


class GDriveBaseError(Exception):
    '''Base GDrive error'''


class GDriveAuthError(WorthRetryingException):
    '''General authentication error'''
    # Worth retrying until we discover which errors are impossible to rectify


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


def build_sns_message(message, root_customer_folder_link, sow_folder_link, project_folder_ids):
    '''Construct SNS message and include info about the fields that were updated'''
    sns_message = {
        'CustomerName': message['CustomerName'],
        'ProjectName': message['ProjectName'],
        'DealId': message['DealId'],
        'DealFieldLinks': {
            'GDriveLink': root_customer_folder_link,
            'SOWLink': sow_folder_link
        },
        'FolderIds': project_folder_ids
    }
    return sns_message


def build_message_attributes(action, stage='lead_in'):
    '''Construct message attributes'''
    if action == 'added.deal':
        action = 'create_folders'

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


def create_folder(drive, parent_folder_id, folder_name):
    '''Create a single folder in GDrive and return folder id'''
    try:
        folder = drive.CreateFile(dict(
            title=folder_name,
            parents=[dict(id=parent_folder_id)],
            mimeType='application/vnd.google-apps.folder'
        ))
        folder.Upload()
    except Exception as error:
        exc_info = sys.exc_info()
        raise ExternalAPIFailed(error).with_traceback(exc_info[2])

    # try:
    #     folder.Upload()
    # except FileNotUploadedError as erru:
    #     LOGGER.exception(erru)
    # except ApiRequestError:
    #     pass

    return folder['id']



def create_customer_folder_structure(drive, parent_folder_id, customer, project):
    '''Creates _SALES, _ENGINEERING, _DELIVERY folders in the customer folder'''
    try:
        # Create a new folder named Customer in the root parent folder.
        customer_folder_id = create_folder(drive, parent_folder_id, customer)
        file_object = drive.CreateFile({'id': customer_folder_id})
        root_customer_folder_link = file_object['alternateLink']

        # Create _SALES, _ENGINEERING, _DELIVERY, _ACCOUNT folders in the root
        # customer folder. Store folderIds in variables
        child_ids = {}
        for child in ['_SALES', '_ENGINEERING', '_DELIVERY', '_ACCOUNT']:
            child_folder_id = create_folder(drive, customer_folder_id, child)
            child_ids[child] = child_folder_id
    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise GDriveBaseError(error).with_traceback(exc_info[2])

    return customer_folder_id, child_ids, root_customer_folder_link


def create_project_folder_structure(drive, parent_folder_id, customer, project, customer_child_ids):
    '''Creates Project Name: Project folder in each of the child folders
       Then creates required project folders
       Returns dict of project folder ids as well as the sow folder link'''
    # TODO: Refactor in to separate method
    project_folder_ids = {}
    try:
        for (name, child_id) in customer_child_ids.items():
            if name != '_ACCOUNT':
                match = check_child_folder_exists(drive, child_id, 'Project Name: {}'.format(project))
                if not match:
                    folder_id = create_folder(drive, child_id, 'Project Name: {}'.format(project))
                    project_folder_ids.update({name : {'ProjectId': folder_id, 'SubFolders': {}}})
                else:
                    project_folder_ids.update({name : {'ProjectId': match[0]['id'], 'SubFolders': {}}})

        # In _SALES/PROJECT_NAME, create these folders: APN Portal Admin, Deliverables, Meeting_Notes, SOW
        sales_folders = ['APN Portal Admin', 'Deliverables', 'Meeting_Notes', 'SOW']

        # Check if any folders have already been created
        sales_child_list = list_file_object(drive, project_folder_ids['_SALES']['ProjectId'], directory_only=True)
        for folder in sales_child_list:
            project_folder_ids['_SALES']['SubFolders'].update({folder['title'] : folder['id']})

        for folder in sales_folders:
            if folder not in [x['title'] for x in sales_child_list]:
                folder_id = create_folder(drive, project_folder_ids['_SALES']['ProjectId'], folder)
                project_folder_ids['_SALES']['SubFolders'].update({folder : folder_id})

        # In _ENGINEERING/PROJECT_NAME, create these folders: Deliverables, Provided_Documents
        engineering_folders = ['Deliverables', 'Provided_Documents']

        # Check if any folders have already been created
        eng_child_list = list_file_object(drive, project_folder_ids['_ENGINEERING']['ProjectId'], directory_only=True)
        for folder in eng_child_list:
            project_folder_ids['_ENGINEERING']['SubFolders'].update({folder['title'] : folder['id']})

        for folder in engineering_folders:
            if folder not in [x['title'] for x in eng_child_list]:
                folder_id = create_folder(drive, project_folder_ids['_ENGINEERING']['ProjectId'], folder)
                project_folder_ids['_ENGINEERING']['SubFolders'].update({folder : folder_id})

        # In _DELIVERY/PROJECT_NAME, create these folders:
        delivery_folders = ['Weekly_Action_Reports', 'Engagement_Data_Reports', 'Communications', 'Onboarding', 'Who’s Who']

        # Check if any folders have already been created
        deliv_child_list = list_file_object(drive, project_folder_ids['_DELIVERY']['ProjectId'], directory_only=True)
        for folder in deliv_child_list:
            project_folder_ids['_DELIVERY']['SubFolders'].update({folder['title'] : folder['id']})

        for folder in delivery_folders:
            if folder not in [x['title'] for x in deliv_child_list]:
                folder_id = create_folder(drive, project_folder_ids['_DELIVERY']['ProjectId'], folder)
                project_folder_ids['_DELIVERY']['SubFolders'].update({folder : folder_id})

        sow_file_object = drive.CreateFile({'id': project_folder_ids['_SALES']['SubFolders']['SOW']})
        folder_ids = {
            'CustomerFolderId': parent_folder_id,
            'SalesFolder': {
                'RootId': customer_child_ids['_SALES'],
                'ProjectId': project_folder_ids['_SALES']['ProjectId'],
                'SubFolders': project_folder_ids['_SALES']['SubFolders']
            },
            'DeliveryFolder': {
                'RootId': customer_child_ids['_DELIVERY'],
                'ProjectId': project_folder_ids['_DELIVERY']['ProjectId'],
                'SubFolders': project_folder_ids['_DELIVERY']['SubFolders']
            },
            'EngineeringFolder': {
                'RootId': customer_child_ids['_ENGINEERING'],
                'ProjectId': project_folder_ids['_ENGINEERING']['ProjectId'],
                'SubFolders': project_folder_ids['_ENGINEERING']['SubFolders']
            },
            'AccountFolder': {
                'RootId': customer_child_ids['_ACCOUNT']
            }
        }


    except Exception as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise ExternalAPIFailed(error).with_traceback(exc_info[2])

    return folder_ids, sow_file_object['alternateLink']


def update_customers_table(customer_name, project_name, folder_ids):
    '''Updates the gdrive-customers DDB table with the CustomerName, ProjectName, and FolderIds'''
    table = DDB.Table('gdrive-customers')

    try:
        response = table.update_item(
            Key={
                'customer': customer_name,
                'project': project_name
            },
            UpdateExpression="set folder_ids = :f",
            ExpressionAttributeValues={':f': folder_ids},
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as error:
        LOGGER.exception(error)
        exc_info = sys.exc_info()
        raise DynamoDBError(error).with_traceback(exc_info[2])


def get_customer_child_folders(drive, folder_id):
    '''Returns dict of top-level child folders for a customer and the
       customer root folder link'''
    customer_child_folders = ['_SALES', '_ENGINEERING', '_DELIVERY', '_ACCOUNT']
    customer_folder_list = list_file_object(
        drive, folder_id, directory_only=True)
    customer_child_ids = {}

    for f in customer_child_folders:
        # check if the correct folders exist; create them if not
        if f not in [x['title'] for x in customer_folder_list]:
            new_folder_id = create_folder(drive, folder_id, f)
            customer_child_ids.update({f: new_folder_id})

    for f in customer_folder_list:
        customer_child_ids.update({f['title'] : f['id']})

    # get customer root folder link
    file_object = drive.CreateFile({'id': folder_id})

    return customer_child_ids, file_object['alternateLink']


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


def format_response(message):
    ''' Format the message to be returned as the response body '''
    message = {'message': message}
    return json.dumps(message)


def check_child_folder_exists(drive, parent_folder_id, title):
    '''Check if a folder with the given title exists within the parent folder'''
    folder_list = list_file_object(
        drive,
        parent_folder_id,
        directory_only=True
    )
    match = [x for x in folder_list if x['title'] == title]
    return match


def lambda_handler(event, context):
    '''GDrive Create Folders entry'''
    # googleapiclient throws an inconsequential warning that causes the function to
    # error out so we silence it here
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    response = {'status': 200}

    print('Event received: {}'.format(event))

    try:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        customer_name = message['CustomerName']
        project_name = message['ProjectName']
        action = event['Records'][0]['Sns']['MessageAttributes']['action']['Value']

        # Initialize GDrive authentication
        drive = init_auth()

        # Check if a Customer root folder exists
        match = check_child_folder_exists(drive, GDRIVE_PARENT_FOLDER_ID, customer_name)
        if match:
            customer_folder_id = match[0]['id']
            # get customer child folders and root customer folder link
            customer_child_ids, root_customer_folder_link = get_customer_child_folders(drive, customer_folder_id)
        else:
            customer_folder_id, customer_child_ids, root_customer_folder_link = create_customer_folder_structure(drive, GDRIVE_PARENT_FOLDER_ID, customer_name, project_name)

        project_folder_ids, sow_folder_link = create_project_folder_structure(drive, customer_folder_id, customer_name, project_name, customer_child_ids)

        # Update Gdrive Customers DynamoDB Table
        update_customers_table(customer_name, project_name, project_folder_ids)

        # Send SNS message with customer_name,project_name,RootCustomerFolderLink
        # and SOWLink to Gdrive SNS topic
        sns_message = build_sns_message(message, root_customer_folder_link, sow_folder_link, project_folder_ids)

        # Publish a message to GDrive Topic
        message_attributes = build_message_attributes(action)
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
