import json
import boto3
import urllib3
import botocore.httpsession
import uuid
from datetime import datetime, timedelta
import os

session = botocore.httpsession.URLLib3Session()
http = urllib3.PoolManager()
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    client_id= os.getenv('CLIENT_ID')
    client_secret= os.getenv('CLIENT_SECRET')
    login_hint = os.getenv('LOGIN_HINT')
    day = False
    sma_resp = sma_request(client_id, client_secret, login_hint, day)
    send_data_to_kinesis(sma_resp)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def req_builder(access_token, uri, method='GET'):
    auth = 'Bearer '+ access_token
    req = http.request(
        method,
        uri,
        headers = {
            # "Content-Type": "application/json",
            "Authorization": auth
            
        }
    )
    return json.loads(req.data)
    
def custom_flow_auth(client_id, client_secret, login_hint):
    # Onboard new plants/clients
    sma_one = sma_step_one(client_id, client_secret)
    # refresh_token = sma_one['refresh_token']
    # sma_refresh = sma_step_refresh(client_id, client_secret, refresh_token)
    # sma_two = sma_step_two(login_hint, sma_refresh['access_token'])
    sma_two = sma_step_two(login_hint, sma_one['access_token'])
    sma_three = sma_step_three(login_hint, sma_one['access_token'])
    sma_four = sma_step_four(login_hint, sma_one['access_token'])
    return sma_one['access_token']
    
def sma_request(client_id, client_secret, login_hint, day=False):
    sma_one = sma_step_one(client_id, client_secret)
    # refresh_token = sma_one['refresh_token']
    # sma_refresh = sma_step_refresh(client_id, client_secret, refresh_token)
    # sma_two = sma_step_two(login_hint, sma_refresh['access_token'])
    # sma_two = sma_step_two(login_hint, sma_one['access_token'])
    # sma_three = sma_step_three(login_hint, sma_refresh['access_token'])
    # sma_four = sma_step_four(login_hint, sma_refresh['access_token'])
    plants = sma_get_plants(sma_one['access_token'], day)
    return plants
    
def sma_step_one(client_id, client_secret):
    req_body = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    req = http.request(
        'POST',
        'https://auth.smaapis.de/oauth2/token',
        fields = req_body,
        encode_multipart=False,
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return json.loads(req.data)

def sma_step_refresh(client_id, client_secret, refresh_token):
    grant_type='refresh_token'
    method = 'POST'
    uri = 'https://auth.smaapis.de/oauth2/token'
    auth = 'Bearer ' + refresh_token
    req_body = {
        'grant_type': grant_type,
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token
    }
    req = http.request(
        method,
        uri,
        fields = req_body,
        encode_multipart=False,
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': auth
            
        }
    )
    return json.loads(req.data)
    
    
def sma_step_two(login_hint, access_token):
    uri = 'https://async-auth.smaapis.de/oauth2/v2/bc-authorize'
    auth = 'Bearer '+ access_token
    print(['acc tok: ', access_token])
    method = 'POST'
    req_body = {
        "loginHint": os.getenv('LOGIN_HINT')
    }
    req = http.request(
        method,
        uri,
        body = json.dumps(req_body),
        headers = {
            "Content-Type": "application/json",
            "Authorization": auth
            
        }
    )
    return json.loads(req.data)
    
def sma_step_three(login_hint, access_token):
    uri = 'https://async-auth.smaapis.de/oauth2/v2/bc-authorize/' + login_hint + '/status'
    auth = 'Bearer '+ access_token
    method = 'PUT'
    req_body = {
        "state": "Accepted"
    }
    req = http.request(
        method,
        uri,
        body = json.dumps(req_body),
        headers = {
            "Content-Type": "application/json",
            "Authorization": auth
            
        }
    )
    print(['step 3 data: ', json.loads(req.data)])  
    return json.loads(req.data)
    
def sma_step_four(login_hint, access_token):
    uri = 'https://async-auth.smaapis.de/oauth2/v2/bc-authorize/' + login_hint
    auth = 'Bearer '+ access_token
    method = 'GET'
    req = http.request(
        method,
        uri,
        headers = {
            "Content-Type": "application/json",
            "Authorization": auth
            
        }
    )

    return json.loads(req.data)
  
  
def sma_get_plants(access_token, day=False):
    uri = 'https://monitoring.smaapis.de/v1/plants/'
    auth = 'Bearer '+ access_token
    method = 'GET'
    req = http.request(
        method,
        uri,
        headers = {
            "Content-Type": "application/json",
            "Authorization": auth
            
        }
    )
    plants = json.loads(req.data)['plants']
    plants_details_list = [] # List of plants with their respective details
    for plant in plants:
        plant_id = plant['plantId'] 
        plant_details= get_plant_details(plant_id, access_token, day)
        plant_devices= get_plant_devices(plant_id, access_token, day)

        for energy_set in plant_details['set']:
            set_details = {
            'plantOrDevice': 'plant',
            **plant_details['plant'],
            'setType':plant_details['setType'],
            **energy_set
            }
            plants_details_list.append(set_details)
        for device_obj in plant_devices:
            device_obj['plantOrDevice'] = 'device'
            device_obj['plantId'] = plant_id
            device_obj['name'] = plant['name']
            plants_details_list.append(device_obj)

            
    return plants_details_list
    

    
def get_plant_details(plant_id, access_token, day=False):
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    if (day) :
        yesterday = day   
    uri = 'https://monitoring.smaapis.de/v2/plants/'+plant_id+'/measurements/sets/EnergyBalance/day?date='+yesterday
    auth = 'Bearer '+ access_token
    method = 'GET'
    req = http.request(
        method,
        uri,
        headers = {
            # "Content-Type": "application/json",
            "Authorization": auth
            
        }
    )
    return json.loads(req.data)
    
def get_plant_devices(plant_id, access_token, day=False):
    if (day):
     yesterday = day   
    
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    devices_uri = 'https://monitoring.smaapis.de/v1/plants/'+plant_id+'/devices'
    devices_req = req_builder(access_token, devices_uri, 'GET')
    devices = devices_req['devices']
    device_detail_list = []
    for device in devices:
        device_id = device['deviceId']
        device_uri = 'https://monitoring.smaapis.de/v1/devices/'+device_id
        device_req = req_builder(access_token, device_uri, 'GET')
        detail_obj = device_req['details']
        device_power_uri = 'https://monitoring.smaapis.de/v1/devices/'+device_id+'/measurements/sets/EnergyAndPowerPv/day?date='+yesterday
        device_power_req = req_builder(access_token, device_power_uri, 'GET')
        detail_obj["setType"] = device_power_req["setType"]
        # detail_obj["set"] = device_power_req["set"]
        if len(device_power_req["set"]) > 0:
            detail_obj.update(device_power_req["set"][0])
        
        device_detail_list.append(detail_obj)
    return device_detail_list
    
def send_data_to_kinesis(data):
    resp = kinesis_client.put_record(
        StreamName=os.getenv('STREAM_NAME'),
        Data=json.dumps(data),
        PartitionKey=str(uuid.uuid4())
    )
    print(['kinesis resp: ', resp])