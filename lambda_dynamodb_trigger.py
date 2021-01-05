import json, os, boto3
from botocore.vendored import requests
from datetime import datetime

#Located in Lambda Environment Variable
TOKEN = os.environ['SLACK_OAUTH_TOKEN']
REGION_NAME = os.environ['REGION_NAME']
TABLE_NAME = os.environ['TABLE_NAME']

SLACK_API_URL = 'https://slack.com/api/users.profile.get'

def get_user_info(user):
    url = SLACK_API_URL
    data = {
        'token':TOKEN,
        'user':user
    }
    data["user"]=user
    res = requests.post(url=url,data=data)
    return json.loads(res.text)['profile']
    
def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        if record['eventName'] == "INSERT":
            new_image = record['dynamodb']['NewImage']
            user_id = new_image['user']['S']
            reaction_event_ts = float(new_image['event_ts']['S'])
            post_event_ts = float(new_image['item']['M']['ts']['S'])
            
            #Seconds
            time_diff = int(reaction_event_ts-post_event_ts) 
            
            dt = datetime.fromtimestamp(float(reaction_event_ts))
        
            #e.g., z20200103
            reaction_date = str(dt.date().strftime('z%Y%m%d'))
            reaction_time = str(dt.strftime('%H:%M:%S'))
            
            #If reaction time is not later than 1hour
            if time_diff <= 3600:
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

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
