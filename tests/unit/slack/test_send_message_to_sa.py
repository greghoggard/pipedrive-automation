# pylint: disable=protected-access
# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name
import json
import os
import requests

import boto3
from moto import mock_sns, mock_sts
import pytest

import Components.slack.send_message_to_sa as h

SNS_TOPIC_NAME = "mock-slack-component-topic"
API_TOKEN_PATH = "/Slack/Labs/slack_api_token"



# @pytest.fixture()
# def event(event_file=EVENT_FILE):
#     '''Trigger event'''
#     with open(event_file) as f:
#         return json.load(f)

@pytest.fixture()
def sns_client():
    '''SNS client'''
    return boto3.client('sns')


# def test_sanitize_slack_channel_name_clean():
#     '''Test getting SA slack ID'''
#     sa_east = 'UMU2DE6VD'
#     sa_west = 'UMENPEX0B'
#     r = h.get_sa_slack_id(region='USEAST')
#     assert r == sa_east

#     r = h.get_sa_slack_id(region='USWEST')
#     assert r == sa_west

#     r = h.get_sa_slack_id()
#     assert r == sa_east

#     r = h.get_sa_slack_id(region='NONexistent')
#     assert r == sa_east