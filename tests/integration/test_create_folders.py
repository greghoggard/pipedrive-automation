# pylint: disable=protected-access
# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name
import json
import os

import boto3
import pytest

EVENT_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    'events',
    'added_deal.json'
)


@pytest.fixture()
def event(event_file=EVENT_FILE):
    '''Trigger event'''
    with open(event_file) as f:
        return json.load(f)


@pytest.fixture()
def cfn_stack_name():
    '''Return name of stack to get Lambda from'''
    return 'placeholder'


@pytest.fixture()
def lambda_client():
    '''Lambda client'''
    return boto3.client('lambda')


@pytest.fixture()
def lambda_function(cfn_stack_name):
    '''Return Lambda function name'''
    return '-'.join([cfn_stack_name, 'GdriveFolderCreation'])


def test_handler(lambda_client, lambda_function, event):
    '''Test handler'''
    r = lambda_client.invoke(
        FunctionName=lambda_function,
        InvocationType='RequestResponse',
        Payload=json.dumps(event).encode()
    )

    lambda_return = r.get('Payload').read()
    slack_response = json.loads(lambda_return).get('slack_response')

    assert slack_response.get('ok') is True

    # verify CustomerName folder exists with parent of Customers
    # verify that _SALES, _DELIVERY, _ENGINEERING folders exist with parent of CustomerName
    # verify ProjectName folder exists in each of _SALES, _DELIVERY, _ENGINEERING with parent of CustomerName
    # verify that Account, APN Portal Admin, Deliverables, Meeting_Notes, SOW folders exist with parent of _SALES/ProjectName
    # verify that Deliverables, Provided_Documents folders exist with parent of _ENGINEERING/ProjectName
    # verify that Weekly_Action_Reports, Engagement_Data_Reports, Onboarding, Who's Who, Communications folders exist with parent of _DELIVERY/ProjectName
