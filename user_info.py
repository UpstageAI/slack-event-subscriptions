import json, os, boto3
import requests
import os
#from botocore.vendored import requests
from datetime import datetime, timedelta, timezone

SLACK_OAUTH_TOKEN = os.getenv ('SLACK_OAUTH_TOKEN')
SLACK_API_URL = 'https://slack.com/api/users.profile.get'

def get_user_info(user_id):
    url = SLACK_API_URL
    data = {
        'token':SLACK_OAUTH_TOKEN,
        'user':user_id
    }

    res = requests.post(url=url,data=data)
    #FIXME: user not found error
    return json.loads(res.text)['profile']

def get_slack_username(user_id):
    return get_user_info(user_id)['display_name']


if __name__ == '__main__':
    with open('zappa_settings.json') as config_file:
        data = json.load(config_file)
        env_araibles = data['dev']['environment_variables']
        for key in env_araibles:
            os.environ[key] = env_araibles[key]
    SLACK_OAUTH_TOKEN = os.getenv ('SLACK_OAUTH_TOKEN')
    print(get_user_info('U01HYQ2D2VD'))
    print(get_slack_username('U01HYQ2D2VD'))

