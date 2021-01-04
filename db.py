import boto3

def get_event_key(event):
    item = event['item']
    return item['channel'] + ":" + item['ts'] + ":" + event['user'] + ":" + event['reaction'] 

def put_event(event, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
        dynamodb = boto3.resource('dynamodb')


    table = dynamodb.Table('connect_up_slack')
    event['key'] = get_event_key(event)
    response = table.put_item(Item=event)
    return response


if __name__ == '__main__':
    event1 = {'type': 'reaction_added', 'user': 'U017FMWG9CJ', 'item': {'type': 'message', 'channel': 'C01C9EAF4E9', 'ts': '1607731457.022800'}, 'reaction': '+1', 'item_user': 'U017FMWG9CJ', 'event_ts': '1609676655.093400'}
    print(event1)
    print(get_event_key(event1))
    put_event(event1)