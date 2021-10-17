
import os
from boto3.session import Session
from boto3.dynamodb import table
from boto3.dynamodb.conditions import Key


from datetime import datetime as dt
import pprint as pp



import boto3

dynamodb = boto3.resource('dynamodb',
                          aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                          region_name='us-east-1'
)

items = []


old_table = 'lee-county-covid-19-data-development'
table_name = 'lee-county-covid-19-data-preview'

table = dynamodb.Table(old_table)
new_table = dynamodb.Table(table_name)

data_to_move = []

scan_kwargs = {
    }

done = False
start_key = None
while not done:
    if start_key:
        scan_kwargs['ExclusiveStartKey'] = start_key
    response = table.scan(**scan_kwargs)
    for row in response.get('Items', []):
        print(row)
        data_to_move.append(row)
    
    start_key = response.get('LastEvaluatedKey', None)
    done = start_key is None


print(len(data_to_move))

for item in data_to_move:
    #pp.pprint(item)
    item['id'] = item['school'].lower()
    item['name'] = item['school'].replace("-"," ")

pp.pprint(data_to_move)

with new_table.batch_writer() as batch:
    for r in data_to_move:
        batch.put_item(Item=r)



