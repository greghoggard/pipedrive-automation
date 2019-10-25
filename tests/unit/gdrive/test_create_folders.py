# pylint: disable=protected-access
# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name
import json
import os
import logging
import boto3
from moto import mock_sns, mock_sts
import pytest
import pydrive
from pydrive.auth import GoogleAuth, AuthError
from pydrive.drive import GoogleDrive

import Components.gdrive.create_folders as h

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

EVENT_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    'events',
    'added_deal.json'
)

SETTINGS_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    'events',
    'settings.yaml'
)

PARENT_FOLDER_ID = '1CJE2PnQKjH6mO6N9wwZQQ4uDD2VO0cSP'
SNS_TOPIC_NAME = "mock-gdrive-component-topic"


@pytest.fixture()
def event(event_file=EVENT_FILE):
    '''Trigger event'''
    with open(event_file) as f:
        return json.load(f)

@pytest.fixture()
def sns_client():
    '''SNS client'''
    return boto3.client('sns')

@pytest.fixture()
def gdrive_client(settings_file=SETTINGS_FILE):
    '''GDrive client'''
    gauth = GoogleAuth(settings_file=settings_file)
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

def test_init_auth():
    '''Test Gdrive authentication'''
    r = h.init_auth(SETTINGS_FILE)

    assert isinstance(r, pydrive.drive.GoogleDrive)

def test_create_folder(gdrive_client):
    '''Test Gdrive folder creation'''
    drive = gdrive_client
    r = h.create_folder(drive, PARENT_FOLDER_ID, 'pytest')

    file_object = drive.CreateFile({'id': r})

    assert file_object['title'] == 'pytest'
    file_object.Delete()

