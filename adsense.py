import json
import time 
import pandas_gbq
import requests
import datetime
import json
import pandas
import gspread
import string
from google.cloud import bigquery
import sys
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account

key_path = "/home/dima.k/rising-minutia-372107-3f00351690a6.json"

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
bigquery_client = bigquery.Client.from_service_account_json(key_path)


key_path_token = "/home/dima.k/token.json"
f = open(key_path_token, "r")
key_other = f.read()
body = json.loads(key_other)

token_req_test = f'https://www.googleapis.com/oauth2/v4/token'
credentials = requests.post(token_req_test,data=body ).json()

creds = Credentials(credentials['access_token'])
service = build('adsense', 'v2', credentials=creds)

def get_report(start):
    
    end = datetime.datetime.today() - datetime.timedelta(days=1)
    
    result = service.accounts().reports().generate(
            account='accounts/pub-6414989428031727', dateRange='CUSTOM',
            startDate_year=start.year, startDate_month=start.month, startDate_day=start.day,
            endDate_year=end.year, endDate_month=end.month, endDate_day=end.day,
            metrics=['PAGE_VIEWS', 'ESTIMATED_EARNINGS', 'CLICKS'],
            dimensions=['DATE', 'DOMAIN_NAME'],
            orderBy=['+DATE']
    ).execute()

    columns = [i['name'] for i in result['headers']]
    data = []
    for i in result['rows']:
        data.append([i['value'] for i in i['cells']])
        
    return pandas.DataFrame(data, columns = columns)

q = f"""SELECT  MAX(DATE) as date FROM `rising-minutia-372107.ALL_SALES.Adsense` """
last_dt = pandas_gbq.read_gbq(q, project_id='rising-minutia-372107', credentials=gbq_credential) 

start = datetime.datetime.strptime(last_dt['date'][0],"%Y-%m-%d").date() - datetime.timedelta(days=10)
report_raw = get_report(start)

report_raw['ESTIMATED_EARNINGS'] = report_raw['ESTIMATED_EARNINGS'].astype(float)
report_raw['PAGE_VIEWS'] = report_raw['PAGE_VIEWS'].astype(int)
report_raw['CLICKS'] = report_raw['CLICKS'].astype(int)

bigquery_client = bigquery.Client.from_service_account_json(key_path)

q = f"""
    delete from ALL_SALES.Adsense
    where DATE >= '{min(report_raw['DATE'])}'
"""
job = bigquery_client.query(q)
report_raw.to_gbq('ALL_SALES.Adsense', project_id='rising-minutia-372107',chunksize=20000, if_exists='append', credentials=gbq_credential)