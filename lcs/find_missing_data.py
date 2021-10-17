import sendgrid
import os

from sendgrid.helpers.mail import *
import os
from boto3.session import Session
from boto3.dynamodb import table
from boto3.dynamodb.conditions import Key

from datetime import datetime as dt
import pprint as pp

schools = []
session = Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)
dynamodb = session.resource('dynamodb')

school_cache = []
def get_school_ids():
    school_list = []
    table = dynamodb.Table('lee-county-covid-19-schools-development')
    scan_kwargs = {
    }
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        for row in response.get('Items', []):
            #print(row)
            school_cache.append(row)
        
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    for item in school_cache:
        school_list.append(item['school'].lower())
    return school_list



def get_data_after(school_id, the_date):
    
    table = dynamodb.Table('lee-county-covid-19-data-development')
    response = table.query(
        #KeyConditionExpression=Key('date').gt(the_date.isoformat()) & Key('id').eq("*")
        KeyConditionExpression=Key('id').eq(school_id) & Key('date').gt(the_date.strftime("%Y-%m-%d")),
        #KeyConditionExpression=Key('date').gt(the_date.strftime("%Y-%m-%d"))
        #ProjectionExpression="id, #d, #n, new_cases, new_staff, new_students",
        #ExpressionAttributeNames = {'#d': 'date','#n':"name"}
    )
    return response['Items']


def check_school(name, data):

    return True;

def insert_data(data):
    
    item = {
        "id": data['id'],
        "school" : data['school'],
        "name" : data['name'],
        "date": data['date'],
        "new_cases" : int(data['new_cases']),
        "new_students" : int(data['new_students']),
        "new_staff" : int(data['new_staff']),
        "total_cases" : int(data['total_cases']),
        "total_students" : int(data['total_students']),
        "total_staff" : int(data['total_staff']),
        "added" : dt.utcnow().isoformat(),
    }
    


schools = get_school_ids()
pp.pprint(schools)
#check if there's new data

school_counter = 0
data_to_move = []

for school_id in schools:
    school_data =  get_data_after(school_id, dt(2021, 8, 31))
    #pp.pprint(school_data)
    latest_data = school_data[len(school_data)-1]

    '''
    
    print("Current total:      {}".format(school_data[1]['total_cases']))
    print("Previous total:     {}".format(school_data[0]['total_cases']))
    print("")
    print("Current T students: {}".format(school_data[1]['total_students']))
    print("Prev T students:    {}".format(school_data[0]['total_students']))
    print("")
    print("Current T staff:    {}".format(school_data[1]['total_staff']))
    print("Prev T staff:       {}".format(school_data[0]['total_staff']))
    print("")

    print("New Cases:          {}".format(school_data[1]['new_cases']))
    print("Previous New:       {}".format(school_data[0]['new_cases']))

    print("")
    print("New Student Cases:  {}".format(school_data[1]['new_students']))
    print("P Student Cases:    {}".format(school_data[0]['new_students']))
    print("")
    print("New Staff Cases:  {}".format(school_data[1]['new_staff']))
    print("P Staff Cases:    {}".format(school_data[0]['new_staff']))
    '''
    data = {}
    data['id']     = school_data[1]['id']
    data['school'] = school_data[1]['school']
    data['name'] = school_data[1]['name']
    data['date'] = "2021-09-01T00:00:00"

    data['total_cases'] = int(school_data[1]['total_cases']-school_data[1]['new_cases'])
    data['total_students'] = int(school_data[1]['total_students']-school_data[1]['new_students'])
    data['total_staff'] = int(school_data[1]['total_staff']-school_data[1]['new_staff'])

    data['new_cases'] = int(data['total_cases']-school_data[0]['total_cases'])
 
    data['new_students'] = int(data['total_students']-school_data[0]['total_students'])
    data['new_staff'] = int(data['total_staff']-school_data[0]['total_staff'])
    
    if data['new_cases'] < 0 or data['new_students'] < 0 or data['new_staff'] < 0:
        print("------------------------------- {} -------------------------------".format(data['name']))
    
    if data['new_cases'] < 0:
        data['new_cases'] = 0
    if data['new_students'] < 0:
        data['new_students'] = 0
    if data['new_staff'] < 0:
        data['new_staff'] = 0
    
    pp.pprint(data)
    #current total - new case total == old total
    print("*************************************")
    #insert_data(data)

    data_to_move.append(data)

table2 = dynamodb.Table('lee-county-covid-19-data-development')

with table2.batch_writer() as batch:
    for r in data_to_move:
        batch.put_item(Item=r)
    
    #total_missing = school_data[1]['total_cases'] - school_data[0]['total_cases']-school_data[1]['new_cases']
    #print("{} has {} missing cases".format(school_data[1]['name'], total_missing))




   

    

#print ("{} of {} schools had covid cases".format(school_counter, len(schools)))
#check if there was an increase


#send_notification("chrisrill@gmail.com","The Best School", "https://leecountyschools.fyi/three-oaks-elementary", "30 cases", "8/31", "8/31/2021")


