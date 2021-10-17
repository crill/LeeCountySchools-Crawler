from boto3.resources.model import Identifier
import sendgrid
import os

from sendgrid.helpers.mail import *
import os
from boto3.session import Session
from boto3.dynamodb import table
from boto3.dynamodb.conditions import Key

from datetime import datetime as dt
import pprint as pp
from loguru import logger
import time
schools = []
session = Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

dynamodb = session.resource('dynamodb')

#ENV="development"
ENV="production"
EMAIL = True


school_cache = []
email_cache= []

logger.info("Starting Crawler in {} environment".format(ENV))
def get_schools():
    school_list = {}
    table = dynamodb.Table('lee-county-covid-19-schools-{}'.format(ENV))
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
            school_list[row['school'].replace("-"," ")] = row
        
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    return school_list

def get_people():
    school_list = []
    table = dynamodb.Table('lee-county-covid-19-emails-{}'.format(ENV))
    scan_kwargs = {}
    
    done = False
    
    start_key = None
    
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        for row in response.get('Items', []):
            #print(row)
            email_cache.append(row)
        
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    return email_cache

def update_last_notification(id, email, date):
    now=dt.utcnow().isoformat()
    logger.debug("updating last notification for {} at {}".format(email, date))
    
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('lee-county-covid-19-emails-{}'.format(ENV))
    
    response = table.update_item(
        Key={
            'id': id,
            'email': email
        },
        UpdateExpression="set #last_notification=:last_notif, #last_notification_check=:lnc",
        ExpressionAttributeNames={
            '#last_notification': 'last_notification',
            "#last_notification_check" : "last_notification_check"
            
            },
        ExpressionAttributeValues={
            ":last_notif": date,
            ":lnc": now,

        },
        ReturnValues="UPDATED_NEW"
    )

def update_last_notification_check(id, email):
    now=dt.utcnow().isoformat()

    logger.debug("updating last notification check for {} at {}".format(email, now))
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('lee-county-covid-19-emails-{}'.format(ENV))
    response = table.update_item(
        Key={
            'id': id,
            'email': email
        },
        UpdateExpression="set last_notification_check=:lnc",
        ExpressionAttributeValues={
            ":lnc": now
        },
        ReturnValues="UPDATED_NEW"
    )



def build_school_index(people):
    school_subs = {}

    for person in people:
        if not person['active']:
            logger.debug('{} no longer active'.format(person['email']))
            continue

        for school in person['school']:
            if school not in school_subs:
                logger.info("{} first seen, adding to list".format(school))
                school_subs[school] = []

            if 'last_notification' not in person:
                person['last_notification'] = ""
            
            school_subs[school].append( { "email": person['email'], "id" :person['id'], "last_notification": person['last_notification']})

        
    return school_subs
     

'''
def get_data_after(school_id, the_date):
    
    table = dynamodb.Table('lee-county-covid-19-data-{}'.format(ENV))

    response = table.query(
        #KeyConditionExpression=Key('date').gt(the_date.isoformat()) & Key('id').eq("*")
        KeyConditionExpression=Key('id').eq(school_id) & Key('date').gt(the_date.strftime("%Y-%m-%d")),
        #KeyConditionExpression=Key('date').gt(the_date.strftime("%Y-%m-%d"))
        ProjectionExpression="id, #d, #n, new_cases, new_staff, new_students",
        ExpressionAttributeNames = {'#d': 'date','#n':"name"}
    )
    return response['Items']
'''

def send_notification(to, school, school_link, total_cases, date, date_year):

    if not EMAIL:
        logger.debug("EMAIL is False, exiting send")
        return False

    message = Mail(
        from_email=("chris@leecountyschools.fyi"),
        to_emails=to,
        is_multiple=False)

    message.template_id = "d-e81a4c30cf22408389551948263a0917"
    message.dynamic_template_data = {
                        "school": school,
                        "school_link": school_link,
                        "total_cases": total_cases,
                        "date": date,
                        "date_with_year": date_year
                    }


    try:
        sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        if response.status_code == 202:
            return True
    except Exception as e:
        print(e)

    return False

school_data = get_schools()
people_data = get_people()

#pp.pprint(schools)
#check if there's new data

school_counter = 0


index = build_school_index(people_data)

logger.info("{} schools subscribed to".format(len(index)))

for school, emails in index.items():
    logger.debug("processing list for school {}".format(school))
    cases = school_data[school]["new_cases"]

    if cases == 0:
        logger.info("{} school has had 0 cases, skipping".format(school))
        continue

    case_txt =""
    if cases > 0:
        case_txt = "case" if school_data[school]['new_cases'] == 1 else "cases"
    else:
        print("{} did not have any cases")
        for person in emails:
            logger.debug("updating notification check for {}".format(person['email']))
            update_last_notification_check(person['id'], person['email'])

    #pp.pprint(school_data[school])
    the_date = dt.strptime(school_data[school]["latest_date"], "%Y-%m-%dT%H:%M:%S") #needs to change to actual data date
    date_month = the_date.strftime("%-m/%-d")
    date_year = the_date.strftime("%-m/%-d/%Y")
    link = "{}{}".format("https://leecountyschools.fyi/", school.lower().replace(" ","-"))

    for person in emails:
       
        #check if notification has already been sent
        logger.debug("{} {}".format(person['last_notification'],the_date.isoformat() ))
        if person['last_notification'] != the_date.isoformat():
            print("                          sending email to {}".format(person['email']))
            print("                                           {}".format(school))
            print("                                           {}".format(link))
            print("                                           {} {}".format(cases, case_txt))
            print("                                           {} {}".format(date_month, date_year))
            
            sent = send_notification(person['email'],school, link, "{} {}".format(cases, case_txt), date_month, date_year)

            if sent:
                update_last_notification(person['id'], person['email'], the_date.isoformat())
            else:
                logger.info("Email not sent to: {}".format(person['email']))

        else:
            logger.info("notification already sent for {} on {} ".format(person['email'], the_date.isoformat()))
        
        time.sleep(.1)



exit()
for school in school_data:
    #school_data =  get_data_after(school_id, dt(2021, 8, 30))


    
        
        print("{} has new {} new {}".format(school['school'], school['new_cases'], cases))
        school_counter += 1
        

        notification_emails = get_people_who_subscribed(school['school'].replace("-"," "))


print ("{} of {} schools had covid cases".format(school_counter, len(school_data)))
#check if there was an increase


#send_notification("chrisrill@gmail.com","The Best School", "https://leecountyschools.fyi/three-oaks-elementary", "30 cases", "8/31", "8/31/2021")


