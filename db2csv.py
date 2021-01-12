import json, csv, os
import boto3

from user_info import get_slack_username

#Located in Lambda Environment Variable
EVENT_TABLE = os.getenv('TABLE_NAME')
USER_TABLE = EVENT_TABLE+'_users'


def db2csv(query_date=None, sorting_key=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(USER_TABLE)

    sorting_key = sorting_key if sorting_key else "username"

    if query_date:
        response = table.scan(AttributesToGet=['user_id', 'username', "z"+date])
    else:
        response = table.scan()

    ##Organizing the data from dynamodb as a csv format
    users = response['Items']

    columns = set()
    for user in users:
        # update username just in case user has changed their names
        user['username'] = get_slack_username(user['user_id'])
        columns.update(user.keys())

    columns = sorted(list(columns))
    csv_format_content = ""
    csv_format_content+=",".join(columns) +"\n"

    ## Sort by key
    if sorting_key in columns:
        users = sorted(users, key=lambda k: k[sorting_key] if sorting_key in k else "X")

    for user in users:
        row = []
        for column in columns:
            row.append(user.get(column, "X"))
        csv_format_content+=",".join(row) +"\n"
        
    
    return csv_format_content
    

if __name__ == '__main__':
    print(db2csv())
