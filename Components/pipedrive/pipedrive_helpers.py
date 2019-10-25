'''Pipedrive API helper methods'''
import requests


def get_deal_fields(domain, token):
    url = 'https://{}.pipedrive.com/v1/dealFields:(key,name,options)?start=0&api_token={}'.format(domain, token)

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        fields = resp.json()['data']
    except Exception as error:
        raise Exception(error)

    return fields


def get_deal_field(field_map, field_name, current):
    gdrive_fields = ['GDriveLink', 'SOWLink', 'APNPortalOppLink']
    for key in field_map.keys():
        if field_map[key]['name'] == field_name:
            if field_map[key]['name'] in gdrive_fields:
                return current[key]
            return field_map[key][current[key]]


def build_field_map(fields, map_items):
    field_map = {}
    gdrive_fields = ['GDrive Link', 'SOW Link', 'APN Portal Opp Link']
    for field in fields:
        for item in map_items:
            if item in field['name']:
                field_map.update({field['key'] : {'name': field['name'].replace(' ', '')}})
                for index in range(len(field['options'])):
                    field_map[field['key']].update({'{}'.format(field['options'][index]['id']) : field['options'][index]['label']})
        for item in gdrive_fields:
            if item in field['name']:
                field_map.update({field['key'] : {'name': field['name'].replace(' ', '')}})

    return field_map
