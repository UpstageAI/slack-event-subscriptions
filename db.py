import json, os, boto3
import requests
import os
#from botocore.vendored import requests
from datetime import datetime, timedelta, timezone

SLACK_OAUTH_TOKEN = os.getenv ('SLACK_OAUTH_TOKEN')
SLACK_CHECK_CHANNEL = os.getenv('SLACK_CHECK_CHANNEL')
SLACK_API_URL = 'https://slack.com/api/users.profile.get'


def get_event_key(event):
    item = event['item']
    return item['channel'] + ":" + item['ts'] + ":" + event['user'] + ":" + event['reaction'] 

def put_event(event, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('connect_up_slack')
    event['key'] = get_event_key(event)
    response = table.put_item(Item=event)
    return response


def get_user_info(user):
    url = SLACK_API_URL
    data = {
        'token':SLACK_OAUTH_TOKEN,
        'user':user
    }
    data["user"]=user
    res = requests.post(url=url,data=data)
    #FIXME: user not found error
    return json.loads(res.text)['profile']

def put_attendance(event, channel_id=SLACK_CHECK_CHANNEL, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('connect_up_slack_users')

    if channel_id:
        event_channel_id = event['item']['channel']
        if channel_id != event_channel_id:
            print(event_channel_id + " not found!")
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
        username = get_user_info(user_id)['display_name']
        # This condition should be changed
        if "부캠" in username or True:
            item={
                'user_id': user_id,
                'username': username,
                reaction_date: reaction_time
            }
            table.put_item(Item=item)


if __name__ == '__main__':
    event1 = {'type': 'reaction_added', 'user': 'U017FMWG9CJ', 'item': {'type': 'message', 'channel': 'C01C9EAF4E9', 'ts': '1607731457.022800'}, 'reaction': '+1', 'item_user': 'U017FMWG9CJ', 'event_ts': '1609676655.093400'}
    print(event1)
    print(get_event_key(event1))
    put_event(event1)
    put_attendance(event1, channel_id='C01C9EAF4XX')
    put_attendance(event1, channel_id='C01C9EAF4E9')