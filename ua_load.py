from ga_connector_click import ga_connect
import datetime
import json
import os
import pandas
import pandas_gbq
import sys
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from numpy import dtype
from oauth2client.service_account import ServiceAccountCredentials
import csv
import datetime
import json
import numpy
import pandas
import requests
import string
import sys
import urllib
import os
import pandas
import time
def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "work_scripts/rising-minutia-372107-3f00351690a6.json"
key_path =  "work_scripts/rising-minutia-372107-3f00351690a6.json"
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
q_schema = 'SELECT * FROM UA_BACKGROUND.INFORMATION_SCHEMA.TABLES;'


tables  = {'sessions': {'dimetions': [
                        {'name': 'ga:sourceMedium'},
                        {'name': 'ga:campaign'},
                        {'name': 'ga:deviceCategory'},
                        {'name': 'ga:landingPagePath'},
                        {'name': 'ga:region'}
                     ],
            'metrics':  [
                     {'expression': 'ga:sessions'},
                     {'expression': 'ga:users'}
                     ],
            'filters': ''},
'pages': {'dimetions': [
                        {'name': 'ga:pagePath'},
                        {'name': 'ga:deviceCategory'},
                     ],
            'metrics':  [
                     {'expression': 'ga:timeOnPage'},
                     {'expression': 'ga:exits'},
                     {'expression': 'ga:pageviews'}
                     ],
            'filters': ''},
'events': {'dimetions': [
                        {'name': 'ga:eventCategory'},
                        {'name': 'ga:eventAction'},
                        {'name': 'ga:eventLabel'},
                        {'name': 'ga:deviceCategory'}
                     ],
            'metrics':   [
                     {'expression': 'ga:totalEvents'},
                     {'expression': 'ga:uniqueEvents'},
                     {'expression': 'ga:sessionsWithEvent'}
                     ],
            'filters': ''}}

account = [   
['radaris_com', '44576131', datetime.date(2011, 4, 8)],
 ['homeflock_com', '120741846', datetime.date(2016, 4, 22)],
 ['trustoria_com', '99443555', datetime.date(2015, 3, 17)],
 ['phoneowner_com', '88657488', datetime.date(2014, 8, 4)],
 ['rehold_com', '93418606', datetime.date(2014, 11, 4)],
 ['homemetry_com', '123494040', datetime.date(2016, 6, 8)],
 ['bizstanding_com', '108031612', datetime.date(2015, 9, 7)],
 ['connexy_com', '272663387', datetime.date(2022, 8, 5)]
]

for acc in test_acc:
    print(acc)
    for table in tables:
        table_loaded = acc[0]+'_'+table
        print(table_loaded)
        bq_tables = pandas_gbq.read_gbq(q_schema, project_id='rising-minutia-372107', credentials=gbq_credential) 
        start_date = acc[2]
        if table_loaded in list(bq_tables['table_name']):
            q_date = f'SELECT max(date) as first_dt FROM UA_BACKGROUND.{table_loaded};'
            first_dt = pandas_gbq.read_gbq(q_date, project_id='rising-minutia-372107', credentials=gbq_credential)['first_dt'][0]
            start_date = datetime.datetime.strptime(first_dt,"%Y-%m-%d").date()  
            
        dates_to_load = date_pairs(start_date, datetime.datetime.today().date())
        ga_conc = ga_connect(acc[1])
        for dates in dates_to_load:
            UA_report2 = ga_conc.report_pd([[dates[0],dates[1]]], tables[table])
            UA_report2['date'] = dates[0]
            UA_report2.to_gbq(f'UA_BACKGROUND.{table_loaded}', project_id='rising-minutia-372107',chunksize=20000, if_exists='append', credentials=gbq_credential)