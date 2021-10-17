
import requests
from bs4 import BeautifulSoup
import os
from boto3.session import Session
from boto3.dynamodb import table

from datetime import datetime as dt
import pprint as pp

#ENV="development"
ENV="production"

url = "https://cir.leeschools.net/totals"

resp = requests.get(url)

if resp.status_code != 200:
    exit("did not get page: status== {}".format(resp.status_code))
html = resp.text

session = Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

def update_schools_table(item):
    '''
     
    var params = {
        TableName: 'lee-county-covid-19-emails-'+ENVIRONMENT,
        Key: {
            "id": { "S" : hash },
            "email": {S : email}
        },
        UpdateExpression: "SET #attrName = list_append(if_not_exists(#attrName, :empty_list), :attrValue)",
        ExpressionAttributeNames : {
                        "#attrName" : "school",
        
        },
        ExpressionAttributeValues: {
                ":attrValue": {"L":[{"S":school}]},
                ":empty_list": {"L":[{"S":school}]},
            },
        ReturnValues: "UPDATED_NEW"
            
    };
    '''
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('lee-county-covid-19-schools-{}'.format(ENV))
    response = table.update_item(
        Key={
            'school': item['name'].replace(" ", "-"),
        },
        UpdateExpression="set id=:id, new_cases=:new_cases, new_students=:new_students, new_staff=:new_staff, total_cases=:total_cases, total_students=:total_students, total_staff=:total_staff, updated=:updated, latest_date=:latest_date",
        ExpressionAttributeValues={
            ":id": item['id'],
            ":new_cases" : item['new_cases'],
            ":new_students" : item['new_students'],
            ":new_staff" : item['new_staff'],
            ":total_cases" : item['total_cases'],
            ":total_students" : item['total_students'],
            ":total_staff" : item['total_staff'],
            ":updated": item["added"],
            ":latest_date": item['date']
        },
        ReturnValues="UPDATED_NEW"
    )




 
#print(html)

soup = BeautifulSoup(html, 'html.parser')

table = soup.find('table')

thead = table.find('thead')
data_date = thead.find('th').text
data_date = data_date.replace("COVID-19 Cases for","")
data_date =data_date.strip()
print("date to parse: {}".format(data_date))
the_date = dt.strptime(data_date, "%B %d, %Y")

print(the_date)

with open("./html_files/lc-covid-{}".format(the_date.strftime("%Y-%m-%d")), 'w') as f:
    f.write(html)

items = []
added_at = dt.utcnow().isoformat()


for row in table.find("tbody").find_all("tr"):
    col = row.findAll('td')

    item = {
        "id": row.find('th').getText().lower().replace(" ","-"),
        "school" : row.find('th').getText().replace(" ","-"),
        "name" : row.find('th').getText(),
        "date": the_date.isoformat(),
        "new_cases" : int(col[0].getText()),
        "new_students" : int(col[1].getText()),
        "new_staff" : int(col[2].getText()),
        "total_cases" : int(col[3].getText()),
        "total_students" : int(col[4].getText()),
        "total_staff" : int(col[5].getText()),
        "added" : added_at
    }
    items.append(item)

    update_schools_table(item)

dynamodb = session.resource('dynamodb')
table = dynamodb.Table('lee-county-covid-19-data-{}'.format(ENV))

with table.batch_writer() as batch:
    for r in items:
        batch.put_item(Item=r)

'''
table2 = dynamodb.Table('lee-county-covid-19-data-preview')

with table2.batch_writer() as batch:
    for r in items:
        batch.put_item(Item=r)

table3 = dynamodb.Table('lee-county-covid-19-data-production')

with table3.batch_writer() as batch:
    for r in items:
        batch.put_item(Item=r)
'''