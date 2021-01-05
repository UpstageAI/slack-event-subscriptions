#### Required permissions for lambda role
# S3 upload
# Dynamodb Read

#### Required setting for lambda
# Enviromnet variable for SLACK OAUTH TOKEN
# Long enough time limit: calling slack api might take long

import json, csv, os
import boto3
from datetime import timedelta, datetime
from boto3.dynamodb.conditions import Key
from botocore.vendored import requests


#Located in Lambda Environment Variable
TOKEN = os.environ['SLACK_OAUTH_TOKEN']

SLACK_API_URL = 'https://slack.com/api/users.profile.get'
REGION_NAME = "ap-northeast-2"
TABLE_NAME = 'connect_up_slack'
BUCKET_NAME = 'connect-up-slack-attend'

# To get the time range for attendance check
# takes today(datetime.datetime) to calculate the timestamp
# e.g., if the function is called at 2020-01-05 23:59:00, 
# the range will be (2020-01-05 00:00:00 - 2020-01-06 00:00:00)
def get_time_range(today):
    start_dt = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = datetime.timestamp(start_dt + timedelta(hours=24))
    start_dt = datetime.timestamp(start_dt)
    return str(start_dt), str(end_dt)

# To get the users' event from DynamoDB based on time range from the above function
def get_users(today):
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
    table = dynamodb.Table(TABLE_NAME)
    start, end = get_time_range(today)
    fe = Key('event_ts').between(start,end)
    res = table.scan(FilterExpression=fe)
    return res['Items']

# To get each user's information from Slack 
def get_user_info(user):
    url = SLACK_API_URL
    data = {
        'token':TOKEN,
        'user':user
    }
    data["user"]=user
    res = requests.post(url=url,data=data)
    return json.loads(res.text)['profile']
    

# To save the attendance list as a csv and upload to S3
def save_as_csv(attendee_list, date):
    with open("/tmp/csv_file.csv", "w+") as f:
        temp_csv_file = csv.writer(f)
        temp_csv_file.writerow(["Name", date])
        for name in attendee_list:
            temp_csv_file.writerow([name, 1])
    client = boto3.client('s3',region_name=REGION_NAME)
    client.upload_file('/tmp/csv_file.csv', BUCKET_NAME, str(date)+'.csv')



def lambda_handler(event, context):
    attendee = []

    
    today = datetime.now()
    users = get_users(today)
    today = datetime.now().date()
    for user in users:
        profile = get_user_info(user['user'])
        
        #This condition can be changed.
        #For testing, we accept all users.
        if '_부캠_' in profile['display_name'] or True:
            name = profile['display_name']
            attendee.append(name)
        else: #if the user is not a student, we pass that user
            continue
    
    #Remove duplicated user ans sort by name
    final_list = sorted(list(set(attendee)))
    save_as_csv(final_list, today)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
