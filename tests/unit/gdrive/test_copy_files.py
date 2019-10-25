# # pylint: disable=protected-access
# # pylint: disable=wrong-import-position
# # pylint: disable=redefined-outer-name
# import json
# import os
# import logging
# import boto3
# from moto import mock_sns, mock_sts
# import pytest
# import pydrive
# from googleapiclient.errors import HttpError
# from pydrive.auth import GoogleAuth, AuthError
# from pydrive.drive import GoogleDrive
# from datetime import datetime
# import fiscalyear
# from fiscalyear import FiscalDateTime

# import Components.gdrive.copy_files_lead_in as lead_in
# from Components.gdrive.copy_files_lead_in import GDriveBaseError as LeadInError
# from Components.gdrive.copy_file_lead_validation import GDriveBaseError as LeadValidationError
# import Components.gdrive.copy_file_lead_validation as lead_validation
# import Components.gdrive.copy_file_solution_development as solution_development

# logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
# fiscalyear.START_MONTH = 4
# FISCAL_YEAR = FiscalDateTime.now()

# SETTINGS_FILE = os.path.join(
#     os.path.dirname(__file__),
#     '..',
#     '..',
#     'events',
#     'settings.yaml'
# )

# PARENT_FOLDER_ID = '1CJE2PnQKjH6mO6N9wwZQQ4uDD2VO0cSP'
# SNS_TOPIC_NAME = "mock-gdrive-component-topic"

# @pytest.fixture()
# def sns_client():
#     '''SNS client'''
#     return boto3.client('sns')

# @pytest.fixture(scope='module')
# def gdrive_client(settings_file=SETTINGS_FILE):
#     '''GDrive client'''
#     gauth = GoogleAuth(settings_file=settings_file)
#     gauth.ServiceAuth()
#     return GoogleDrive(gauth)

# # @pytest.fixture()
# def setup_folder_structure(gdrive_client, parent_folder_id=PARENT_FOLDER_ID, customer_name='pytest', project_name='copy_files'):
#     drive = gdrive_client
#     cust_folder_id = create_folder(drive, parent_folder_id, customer_name)
#     sales_folder_id = create_folder(drive, cust_folder_id, '_SALES')
#     sales_proj_id = create_folder(drive, sales_folder_id, 'Project Name: {}'.format(project_name))
#     eng_folder_id = create_folder(drive, cust_folder_id, '_ENGINEERING')
#     eng_proj_id = create_folder(drive, eng_folder_id, 'Project Name: {}'.format(project_name))
#     delivery_folder_id = create_folder(drive, cust_folder_id, '_DELIVERY')
#     deliv_proj_id = create_folder(drive, delivery_folder_id, 'Project Name: {}'.format(project_name))
#     account_folder_id = create_folder(drive, cust_folder_id, '_ACCOUNT')
#     folder_ids =  {
#         "CustomerFolderId": cust_folder_id,
#         "SalesFolder": {
#             "RootId": sales_folder_id,
#             "ProjectId": sales_proj_id,
#             "SubFolders": {}
#         },
#         "DeliveryFolder": {
#             "RootId": delivery_folder_id,
#             "ProjectId": deliv_proj_id,
#             "SubFolders": {}
#         },
#         "EngineeringFolder": {
#             "RootId": eng_folder_id,
#             "ProjectId": eng_proj_id,
#             "SubFolders": {}
#         },
#         "AccountFolder": {
#             "RootId": account_folder_id
#         }
#     }
#     sales_folders = ['APN Portal Admin', 'Deliverables', 'Meeting_Notes', 'SOW']
#     for folder in sales_folders:
#         folder_id = create_folder(drive, sales_proj_id, folder)
#         folder_ids["SalesFolder"]["SubFolders"].update({folder : folder_id})
#     engineering_folders = ['Deliverables', 'Provided_Documents']
#     for folder in engineering_folders:
#         folder_id = create_folder(drive, eng_proj_id, folder)
#         folder_ids["DeliveryFolder"]["SubFolders"].update({folder : folder_id})
#     delivery_folders = ['Weekly_Action_Reports', 'Engagement_Data_Reports', 'Communications', 'Onboarding', 'Who’s Who']
#     for folder in delivery_folders:
#         folder_id = create_folder(drive, deliv_proj_id, folder)
#         folder_ids["EngineeringFolder"]["SubFolders"].update({folder : folder_id})
#     return folder_ids

# @pytest.fixture(autouse=True, scope='module')
# def module_setup_teardown(gdrive_client):
#     print("MODULE SETUP!!!")
#     ids = setup_folder_structure(gdrive_client)
#     yield ids
#     delete_folder(gdrive_client, ids['CustomerFolderId'])
#     print("MODULE TEARDOWN!!!")

# def create_folder(drive, parent_folder_id, folder_name):
#     folder = drive.CreateFile(dict(
#         title=folder_name,
#         parents=[dict(id=parent_folder_id)],
#         mimeType="application/vnd.google-apps.folder"
#     ))
#     folder.Upload()
#     return folder['id']

# def delete_folder(drive, folder_id):
#     folder = drive.CreateFile({'id': folder_id})
#     folder.Delete()

# def test_init_auth():
#     '''Test Gdrive authentication'''
#     r = lead_in.init_auth(SETTINGS_FILE)

#     assert isinstance(r, pydrive.drive.GoogleDrive)

# def test_copy_file(module_setup_teardown, gdrive_client):
#     '''Test copying a file from one folder to another'''
#     drive = gdrive_client
#     source_id = '1bjnQ0PivrnAnnwyVjI_EJhQ37D3NlxscecIEs-DNVWs'
#     dest_title = 'test-copy-file'
#     parent_id = module_setup_teardown['SalesFolder']['RootId']
#     r = lead_in.copy_file(drive, source_id, dest_title, parent_id)

#     assert r['title'] == dest_title
#     r.FetchMetadata()
#     assert r.metadata['parents'][0]['id'] == parent_id

#     with pytest.raises(HttpError) as e:
#         parent_id = None
#         r = lead_in.copy_file(drive, source_id, dest_title, parent_id)

# def test_get_docs_to_copy(module_setup_teardown):
#     '''Test retrieving the ids and dest folders based on stage'''
#     msg = {}
#     msg['CustomerName'] = 'Test Customer'
#     msg['ProjectName'] = 'Test Project'
#     doc_templates = {
#         "AccountPlan": '1PpUvvswKS_-i30WbjrRI5TPmzNkNpiKfdBScIBmFtp0',
#         "RiskLog": '1ZtqXnumOODVGva3_wn5odsGqLsy5DHwxrD7ViPMDbu0',
#         "APNPortalOpp": '1NSA62DMHlZPXiZ1wI37RbnDOjR4rwjArN39mrpVSK2s',
#         "KickOffNotes": '1bjnQ0PivrnAnnwyVjI_EJhQ37D3NlxscecIEs-DNVWs'
#     }
#     docs = {
#         "lead_in": {
#             "{}_Account_Plan_Q{}_{}".format(msg['CustomerName'], FISCAL_YEAR.quarter, datetime.today().strftime('%Y')): {
#                 "id": "1PpUvvswKS_-i30WbjrRI5TPmzNkNpiKfdBScIBmFtp0",
#                 "dest": module_setup_teardown['AccountFolder']['RootId'],
#                 "field_name": 'AccountPlanLink'
#             },
#             "{}_{}_Risk Log".format(msg['CustomerName'], msg['ProjectName']): {
#                 "id": "1ZtqXnumOODVGva3_wn5odsGqLsy5DHwxrD7ViPMDbu0",
#                 "dest": module_setup_teardown['SalesFolder']['ProjectId'],
#                 "field_name": 'RiskLogLink'
#             },
#             "Add New APN Opportunity": {
#                 "id": "1NSA62DMHlZPXiZ1wI37RbnDOjR4rwjArN39mrpVSK2s",
#                 "dest": module_setup_teardown['SalesFolder']['SubFolders']['APN Portal Admin'],
#                 "field_name": 'APNPortalOppLink'
#             }
#         },
#         "lead_validation": {
#             "Pre-KickOff Project Notes": {
#                 "id": "1bjnQ0PivrnAnnwyVjI_EJhQ37D3NlxscecIEs-DNVWs",
#                 "dest": module_setup_teardown['SalesFolder']['SubFolders']['Meeting_Notes'],
#                 "field_name": "KickOffNotesLink"
#             }
#         }
#     }
#     stage = 'lead_in'
#     r1 = lead_in.get_docs_to_copy(msg, doc_templates, stage, module_setup_teardown)

#     assert r1 == docs['lead_in']

#     stage = 'lead_validation'
#     r2 = lead_validation.get_docs_to_copy(stage, doc_templates, module_setup_teardown['SalesFolder']['SubFolders']['Meeting_Notes'])

#     assert r2 == docs['lead_validation']

#     with pytest.raises(LeadInError) as e:
#         stage = 'non_existant_stage'
#         r1 = lead_in.get_docs_to_copy(msg, doc_templates, stage, module_setup_teardown)

#     with pytest.raises(LeadValidationError) as e:
#         stage = 'non_existant_stage'
#         r2 = lead_validation.get_docs_to_copy(stage, doc_templates, module_setup_teardown['SalesFolder']['SubFolders']['Meeting_Notes'])

# def test_copy_files_from_doclist(module_setup_teardown, gdrive_client):
#     '''Test copying files from doclist'''
#     drive = gdrive_client
#     message = {}
#     message['CustomerName'] = 'Test Customer'
#     message['ProjectName'] = 'Test Project'
#     docs = {
#         "{}_Account_Plan_Q{}_{}".format(message['CustomerName'], FISCAL_YEAR.quarter, datetime.today().strftime('%Y')): {
#             "id": "1PpUvvswKS_-i30WbjrRI5TPmzNkNpiKfdBScIBmFtp0",
#             "dest": module_setup_teardown['AccountFolder']['RootId'],
#             "field_name": 'AccountPlanLink'
#         },
#         "{}_{}_Risk Log".format( message['CustomerName'], message['ProjectName']): {
#             "id": "1ZtqXnumOODVGva3_wn5odsGqLsy5DHwxrD7ViPMDbu0",
#             "dest": module_setup_teardown['SalesFolder']['ProjectId'],
#             "field_name": 'RiskLogLink'
#         },
#         "Add New APN Opportunity": {
#             "id": "1NSA62DMHlZPXiZ1wI37RbnDOjR4rwjArN39mrpVSK2s",
#             "dest": module_setup_teardown['SalesFolder']['SubFolders']['APN Portal Admin'],
#             "field_name": 'APNPortalOppLink'
#         }
#     }

#     r = lead_in.copy_files_from_doclist(drive, docs, message)

#     assert r['AccountPlanLink'] != ''

# # def test_get_project_meeting_notes_folder_id(module_setup_teardown, gdrive_client, parent_folder_id=PARENT_FOLDER_ID):
# #     '''Test retrieving project meeting_notes folder id'''
# #     drive = gdrive_client
# #     customer_name='pytest'
# #     project_name='copy_files'
# #     r = lead_validation.get_project_meeting_notes_folder_id(drive, parent_folder_id, customer_name, project_name)
# #     assert r == module_setup_teardown['SalesFolder']['SubFolders']['Meeting_Notes']

# def test_get_project_deliverables_folder_id(module_setup_teardown, gdrive_client, parent_folder_id=PARENT_FOLDER_ID):
#     '''Test retrieving project deliverables folder id'''
#     drive = gdrive_client
#     customer_name='pytest'
#     project_name='copy_files'
#     r = solution_development.get_project_deliverables_folder_id(drive, parent_folder_id, customer_name, project_name)
#     assert r == module_setup_teardown['SalesFolder']['SubFolders']['Deliverables']

# def test_solutions_development_get_docs_to_copy(module_setup_teardown):
#     '''Test retrieving the ids and dest folders based on solution development'''
#     customer_name = 'pytest'
#     expected_title = 'Mphasis Stelligent DevOps Program - pytest'
#     docs = {
#         "Mphasis Stelligent DevOps Program - pytest": {
#             "id": "1rOH5pYrhVnZhN0ChA0xXEJ1QQtuGvvEpg3rZmu62XZk",
#             "dest": module_setup_teardown['SalesFolder']['SubFolders']['Deliverables']
#         },
#         "Mphasis Stelligent Security Program - pytest": {
#             "id": "1D-Be-wlJwdzU80GkdgeDXT9bJH3TnM1dU2Gq9eNJldw",
#             "dest": module_setup_teardown['SalesFolder']['SubFolders']['Deliverables']
#         },
#         "Mphasis Stelligent Enablement Program - pytest": {
#             "id": "1b9IDJ3DTAt3bNkdG-rpjqYS4kYdmCMDOlTHxyqeW74U",
#             "dest": module_setup_teardown['SalesFolder']['SubFolders']['Deliverables']
#         },
#         "Mphasis Stelligent Overview - pytest": {
#             "id": "1PNHyiBavl61oNR3g8iZAXEOvlqz-I5MWX30f6_EsuzY",
#             "dest": module_setup_teardown['SalesFolder']['SubFolders']['Deliverables']
#         }
#     }
#     solution_program = 'DevOps Program'
#     title, doc_list = solution_development.get_docs_to_copy(customer_name, solution_program, module_setup_teardown['SalesFolder']['SubFolders']['Deliverables'])

#     assert title == expected_title
#     assert doc_list == docs['Mphasis Stelligent DevOps Program - pytest']

#     # solution_program = 'Non existent program'
#     # title, doc_list = solution_development.get_docs_to_copy(customer_name, solution_program, module_setup_teardown['SalesFolder']['SubFolders']['Deliverables'])

#     # assert title is None
#     # assert doc_list is None





