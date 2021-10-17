import requests
from bs4 import BeautifulSoup
import os
from boto3.session import Session
from boto3.dynamodb import table

from datetime import datetime as dt
import pprint as pp
url = "https://cir.leeschools.net/totals"

resp = requests.get(url)

if resp.status_code != 200:
    exit("did not get page: status== {}".format(resp.status_code))
html = resp.text

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

items = []
added_at = dt.utcnow().isoformat()
for row in table.find("tbody").find_all("tr"):
    col = row.findAll('td')

    item = {
        "school" : row.find('th').getText().replace(" ","-"),
        "updated": the_date.isoformat(),
        "total_cases" : int(col[3].getText()),
        "total_students" : int(col[4].getText()),
        "total_staff" : int(col[5].getText()),
    }
    items.append(item)

pp.pprint(items)

print(len(items))


session = Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

dynamodb = session.resource('dynamodb')
table = dynamodb.Table('lee-county-covid-19-schools-production')

with table.batch_writer() as batch:
    for r in items:
        batch.put_item(Item=r)
