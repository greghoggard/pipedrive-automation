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

SNS_TOPIC_NAME = "mock-pipedrive-component-topic"
