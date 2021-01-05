import json, csv, os
import boto3

#Located in Lambda Environment Variable
REGION_NAME = os.environ['REGION_NAME']
TABLE_NAME = os.environ['TABLE_NAME']

def lambda_handler(event, _):
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
    table = dynamodb.Table(TABLE_NAME)
    
    filename = 'attendance_all.csv'
    
    if event['rawQueryString']:
        date = event['queryStringParameters'].get('d', None)
        #if valid date is not provided, return all data
        if not date:
            response = table.scan()
        else:
            #otherwise, return the specific date
            filename = 'attendance_{}.csv'.format(date)
            response = table.scan(AttributesToGet=['user_id', 'username', "z"+date])
    else:
        # No querystring, return all data
        response = table.scan()

    ##Organizing the data from dynamodb as a csv format
    users = response['Items']
    columns = set()
    for user in users:
        columns.update(user.keys())

    columns = sorted(list(columns))
    csv_format_content = ""
    csv_format_content+=",".join(columns) +"\n"
        
    for user in users:
        row = []
        for column in columns:
            row.append(user.get(column, "X"))
        csv_format_content+=",".join(row) +"\n"
        
    
    return {
        'statusCode': 200,
        'headers': {
          'Content-Type': 'text/csv',
          'Content-disposition': 'attachment; filename={}'.format(filename)
        },
        'body': csv_format_content
    }
