import json, os, boto3
import requests
import os
#from botocore.vendored import requests
from datetime import datetime, timedelta, timezone



SLACK_OAUTH_TOKEN = os.getenv ('SLACK_OAUTH_TOKEN')
SLACK_CHECK_CHANNEL = os.getenv('SLACK_CHECK_CHANNEL')
EVENT_TABLE = os.getenv('TABLE_NAME', 'connect_up_slack')
USER_TABLE = EVENT_TABLE+'_users'
MSG_TABLE = EVENT_TABLE+'_msgs'
SLACK_API_URL = 'https://slack.com/api/users.profile.get'
KEY_WORD = os.getenv('KEY_WORD')

def create_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    with open('db_schema.json') as json_file:
        schema = json.load(json_file)
    try:
        
        event_table = dynamodb.create_table(
            TableName=EVENT_TABLE,
            KeySchema=schema['event_table']['KeySchema'],
            AttributeDefinitions=schema['event_table']['AttributeDefinitions'],
            ProvisionedThroughput=schema['event_table']['ProvisionedThroughput']
        )
        user_table = dynamodb.create_table(
            TableName=USER_TABLE,
            KeySchema=schema['user_table']['KeySchema'],
            AttributeDefinitions=schema['user_table']['AttributeDefinitions'],
            ProvisionedThroughput=schema['user_table']['ProvisionedThroughput']
        )
        waiter = dynamodb.meta.client.get_waiter('table_exists')
        waiter.wait(TableName=EVENT_TABLE, WaiterConfig=dict(Delay=1, MaxAttempts=30))
        waiter.wait(TableName=USER_TABLE,WaiterConfig=dict(Delay=1, MaxAttempts=30))


        return event_table.table_status, user_table.table_status
    except Exception as e:
        raise e

    

def delete_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')


    try:
        event_table = dynamodb.Table(EVENT_TABLE)
        event_table.delete()
        user_table = dynamodb.Table(USER_TABLE)
        user_table.delete()
        # waiter = dynamodb.meta.client.get_waiter('table_not_exists')
        # waiter.wait(TableName=EVENT_TABLE,WaiterConfig=dict(Delay=1, MaxAttempts=30))
        # waiter.wait(TableName=USER_TABLE,WaiterConfig=dict(Delay=1, MaxAttempts=30))
        return event_table.table_status, user_table.table_status
    except Exception as e:
        raise e


def get_event_key(event):
    item = event['item']
    return item['channel'] + ":" + item['ts'] + ":" + event['user'] + ":" + event['reaction'] 

def put_event(event, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(EVENT_TABLE)
    event['key'] = get_event_key(event)
    response = table.put_item(Item=event)
    print(response)
    return response

def get_msg_key(event):
    return event['channel'] + ':' + event['ts'] 

def put_msg(event, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(MSG_TABLE)
    event['key'] = get_msg_key(event)
    print(MSG_TABLE)
    print(event)
    response = table.put_item(Item=event)
    return response

def get_user_info(user):
    url = SLACK_API_URL
    data = {
        'token':SLACK_OAUTH_TOKEN,
        'user':user
    }
    data['user']=user
    res = requests.post(url=url,data=data)
    result = json.loads(res.text)
    
    if result['ok']:
        return result['profile']['display_name'] if result['profile']['display_name'] else result['profile']['real_name']
    else:
        #raise Exception(result['error'])
        return 'test_user'

def put_attendance(event, channel_id=SLACK_CHECK_CHANNEL, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    

    table = dynamodb.Table(USER_TABLE)
    
    if channel_id:
        event_channel_id = event['item']['channel']
        if channel_id != event_channel_id:
            print(event_channel_id + ' not found!')
            return # do nothing

    event_ts = float(event['event_ts'])
    user_id = event['user']

    ## Timezone: Asia/Seoul
    tz = timezone(timedelta(hours=9))
    dt = datetime.fromtimestamp(float(event_ts), tz)

    #e.g., z20200103
    reaction_date = str(dt.date().strftime('z%Y%m%d'))
    reaction_time = str(dt.strftime('%H:%M:%S'))
    try :
        response = table.get_item(
            Key={'user_id': user_id},
            AttributesToGet=[reaction_date,]
        )
        
        #If the user exists, we add new attribute(date)
        if 'Item' in response:
            #In order to avoid the multiple reaction from the same user
            if not reaction_date in response['Item']:
                table.update_item(
                    Key={ 'user_id': user_id },
                    UpdateExpression='SET {} = :val'.format(reaction_date),
                    ExpressionAttributeValues={ ':val': reaction_time }
                )
        # For the first time
        else:
            username = get_user_info(user_id)

            filter_condition = KEY_WORD in username if KEY_WORD else True

            if filter_condition:
                item={
                    'user_id': user_id,
                    'username': username,
                    reaction_date: reaction_time
                }
                table.put_item(Item=item)
        
        return "SUCCESS"
    except Exception as e:
        raise e



if __name__ == '__main__':
    create_table()
    event1 = {
        "event_ts": "1610429825.000500",
        "item": {
            "channel": "G01K6DQ7TJ4",
            "ts": "1610428341.000200",
            "type": "message"
        },
        "item_user": "U01DQLUCR71",
        "key": "G01K6DQ7TJ4:1610428341.000200:U01DQLUCR71:sunglasses",
        "reaction": "sunglasses",
        "type": "reaction_added",
        "user": "U01DQLUCR72"
    }

    msg1 = {
        'client_msg_id': '5cec7200-d60b-470b-9759-fb34e3d954c3', 
        'type': 'message', 
        'text': 'posting test', 
        'user': 'U01HYQ2D2VD', 
        'ts': '1610488144.006300', 
        'team': 'T01JJ7GJW8Z', 
        'blocks': [{'type': 'rich_text', 'block_id': 'mfyap', 'elements': [{'type': 'rich_text_section', 'elements': [{'type': 'text', 'text': 'posting test'}]}]}], 
        'channel': 'C01JBP6B4G4', 
        'event_ts': '1610488144.006300', 
        'channel_type': 'channel'} 
    
    put_event(event1)
    put_msg(msg1)
    put_attendance(event1, channel_id='G01K6DQ7TJ4')
    put_attendance(event1, channel_id='G01K6DQ7TJ4')
    delete_table()