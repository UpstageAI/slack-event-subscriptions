import json, csv, os
import boto3

#Located in Lambda Environment Variable
REGION_NAME = os.getenv('REGION_NAME', "us-west-2")
USER_TABLE_NAME = os.getenv('USER_TABLE_NAME', "connect_up_slack_user")

def db2csv(query_date=None, sorting_key=None):
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
    table = dynamodb.Table(USER_TABLE_NAME)
    
    sorting_key = sorting_key if sorting_key else "username"

    if query_date:
        response = table.scan(AttributesToGet=['user_id', 'username', "z"+date])
    else:
        response = table.scan()

    ##Organizing the data from dynamodb as a csv format
    users = response['Items']

    columns = set()
    for user in users:
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
