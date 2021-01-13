import json, os, boto3
import requests
import os
#from botocore.vendored import requests
from datetime import datetime, timedelta, timezone



SLACK_OAUTH_TOKEN = os.getenv ('SLACK_OAUTH_TOKEN')
SLACK_CHECK_CHANNEL = os.getenv('SLACK_CHECK_CHANNEL')
EVENT_TABLE = os.getenv('TABLE_NAME')
USER_TABLE = EVENT_TABLE+'_users'
SLACK_API_URL = 'https://slack.com/api/users.profile.get'
KEY_WORD = os.getenv('KEY_WORD')

def create_table(dynamodb=None, event_table_name=None, user_table_name=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    
    event_table_name = event_table_name if event_table_name else EVENT_TABLE
    user_table_name = user_table_name if user_table_name else USER_TABLE

    try:
        dynamodb.create_table(
            TableName=EVENT_TABLE,
            KeySchema=[
                dict(AttributeName= 'key',KeyType='HASH'),
                dict(AttributeName= 'event_ts',KeyType='RANGE'), 
            ],
            AttributeDefinitions=[
                dict(AttributeName='key',AttributeType='S'),
                dict(AttributeName='event_ts',AttributeType='S'),
            ],
            ProvisionedThroughput=dict(ReadCapacityUnits=10,WriteCapacityUnits=10)
        )
        dynamodb.create_table(
            TableName=USER_TABLE,
            KeySchema=[dict(AttributeName='user_id',KeyType='HASH')],
            AttributeDefinitions=[dict(AttributeName='user_id',AttributeType='S')],
            ProvisionedThroughput=dict(ReadCapacityUnits=10,WriteCapacityUnits=10)
        )
    except Exception as e:
        print(e)


def delete_table(dynamodb=None, event_table_name=None, user_table_name=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    event_table_name = event_table_name if event_table_name else EVENT_TABLE
    user_table_name = user_table_name if user_table_name else USER_TABLE

    try:
        dynamodb.Table(event_table_name).delete()
        dynamodb.Table(user_table_name).delete()
    except Exception as e:
        print(e)


def get_event_key(event):
    item = event['item']
    return item['channel'] + ":" + item['ts'] + ":" + event['user'] + ":" + event['reaction'] 

def put_event(event, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')


    table = dynamodb.Table(EVENT_TABLE)
    event['key'] = get_event_key(event)
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
        raise Exception(result['error'])

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


if __name__ == '__main__':
    init_db()
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
    
    put_event(event1)
    put_attendance(event1, channel_id='G01K6DQ7TJ4')
    put_attendance(event1, channel_id='G01K6DQ7TJ4')