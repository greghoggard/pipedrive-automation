# pylint: disable=protected-access
# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name
import json
import os
import requests

import boto3
from moto import mock_sns, mock_sts
import pytest

import Components.slack.create_channel as h

EVENT_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    'events',
    'added_deal.json'
)

SNS_TOPIC_NAME = "mock-slack-component-topic"
API_TOKEN_PATH = "/Slack/Labs/slack_api_token"



@pytest.fixture()
def event(event_file=EVENT_FILE):
    '''Trigger event'''
    with open(event_file) as f:
        return json.load(f)

@pytest.fixture()
def sns_client():
    '''SNS client'''
    return boto3.client('sns')


def test_sanitize_slack_channel_name_clean():
    '''Test sanitizing a clean channel name'''
    channel = 'clean'
    new_channel = h.sanitize_slack_channel_name(channel)
    assert channel == new_channel

    channel = 'clean-channel'
    new_channel = h.sanitize_slack_channel_name(channel)
    assert channel == new_channel

def test_sanitize_slack_channel_name_dirty():
    '''Test sanitizing a dirty channel name'''
    channel = '#Dirty Channel!'
    new_channel = h.sanitize_slack_channel_name(channel)
    assert new_channel == 'dirty-channel'
