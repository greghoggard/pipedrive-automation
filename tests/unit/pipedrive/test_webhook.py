# pylint: disable=protected-access
# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name
import json
import os
import requests

import boto3
from moto import mock_sns, mock_sts
import pytest

import Components.pipedrive.webhook as h

NEW_EVENT_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    'events',
    'added_deal_apigw.json'
)

UPDATE_EVENT_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    'events',
    'update_deal_apigw.json'
)

SNS_TOPIC_NAME = "mock-pipedrive-component-topic"


@pytest.fixture()
def new_event(event_file=NEW_EVENT_FILE):
    '''Trigger event'''
    with open(event_file) as f:
        return json.load(f)

@pytest.fixture()
def update_event(event_file=UPDATE_EVENT_FILE):
    '''Trigger event'''
    with open(event_file) as f:
        return json.load(f)

@pytest.fixture()
def sns_client():
    '''SNS client'''
    return boto3.client('sns')

@mock_sts
@mock_sns
def test_new_deal(sns_client, new_event, sns_topic_name=SNS_TOPIC_NAME):
    '''Test new deal'''
    deal = new_event['body']
    deal_event = deal['event']

    sns_create_topic_resp = sns_client.create_topic(Name=SNS_TOPIC_NAME)
    PIPEDRIVE_SNS_TOPIC_ARN = sns_create_topic_resp.get('TopicArn')

    r = h.new_deal(deal, deal_event, 'lead_in', PIPEDRIVE_SNS_TOPIC_ARN)
    '''Given valid values, it should return success'''
    assert r['statusCode'] == 200

    '''If the Topic ARN is not valid, it should return an error'''
    PIPEDRIVE_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:123456789012:wrong-pipedrive-component-topic'
    with pytest.raises(Exception) as e:
        r = h.new_deal(deal, deal_event, 'lead_in', PIPEDRIVE_SNS_TOPIC_ARN)
