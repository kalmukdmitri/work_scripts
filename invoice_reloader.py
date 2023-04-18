import mysql.connector as mysql
import pandas as pd
import json
import time 
import pandas_gbq
import requests
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import string
from google.cloud import bigquery
import sys

key_path = "rising-minutia-372107-3f00351690a6.json"

gbq_credential = service_account.Credentials.from_service_account_file(key_path,)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
bigquery_client = bigquery.Client.from_service_account_json(key_path)

def query_df(qry, iterate = False, chunk = 5000,iterations = 0):
    cnx = mysql.connect(
    user='guest',
    password='vgFms7-kTl',
    host='172.16.5.71',
    database='hb_acct')
    
    if not iterate:
        cursor = cnx.cursor()
        cursor.execute(qry)
        res = cursor.fetchall()
        field_names = cursor.description
        field_names = [i[0] for i in cursor.description]
        cursor.close()
        cnx.close()
        return pd.DataFrame(res, columns = field_names)
    else:
        final_pd = []
        for i in range(iterations):
            limits = chunk 
            offset = chunk*i
            
            q = qry+f'''
            limit {limits}
            offset {offset}
            '''
            print(q)
            time.sleep(3)
            cursor = cnx.cursor()
            cursor.execute(q)
            res = cursor.fetchall()
            
            field_names = cursor.description
            field_names = [i[0] for i in cursor.description]
            data_pd = pd.DataFrame(res, columns = field_names)
            print(data_pd)
            cursor.close()
            final_pd.append(data_pd)
            
        cnx.close()
        return pd.concat(final_pd).reset_index(drop=True)
        
q = f"""SELECT  MAX(id) as date FROM `rising-minutia-372107.ALL_SALES.invoices_raw` """
last_dt = pandas_gbq.read_gbq(q, project_id='rising-minutia-372107', credentials=gbq_credential) 
last_invoice = last_dt['date'][0]
    
q = f'''select count(*) as len_cnt from invoice 
where id > {last_invoice} '''
invoices_len = query_df(q, iterate = False)

invoices_len = invoices_len.len_cnt[0]
q = f'''select * from invoice 
    where id > '{last_invoice}' '''

if invoices_len > 10000:
    invoices = query_df(q, iterate = True, chunk = 10000,iterations = int(invoices_len/10000)+1)
else:
    invoices = query_df(q, iterate = False)
invoices['amount'] = invoices['amount'].astype(float)
invoices['chargeback_on']= invoices['chargeback_on'].astype(str)
invoices['refund_on']= invoices['refund_on'].astype(str)

invoices.to_gbq('ALL_SALES.invoices_raw', project_id='rising-minutia-372107',chunksize=20000, if_exists='append', credentials=gbq_credential)

q = f'''
select * from invoice_data
where invoice_id >= {min(invoices['id'])}
and invoice_id <= {max(invoices['id'])}
'''
invoice_data = query_df(q, iterate = True, chunk = 5000,iterations = int(invoices_len/5000)+1 )
invoice_data['data'] = invoice_data['data'].apply(lambda x: json.loads(x))
pre_done = []
for rawrow in invoice_data.itertuples():
    rows = {  
                 'fail': '',
                 'product': '',
                 'is_auto_charge': '',
                 'prev_invoice': '',
                 'USER_AGENT': '',
                 'REMOTE_ADDR': '',
                 'discount': '',
                 'criteria': '',
                 'radaris_id': '',
                 'pf_api_version': '',
                 'animation_mark': '',
                 'cardinal_cca': '',
                 'original_radaris_id': '',
                 'inapp': '',
                 'apple_inapp': '',
                 'qa': '',
                 'google_inapp': '',
                 'inapp_sub': '',
                 'google_sub_verify': '',
                 'my_membership': '',
                 'apple_sub_verify': '',
                 'kount': ''}
    
    for cols in rows.keys():
        rows[cols] = rawrow.data[cols] if cols in rawrow.data else ''
    rows['invoice_id'] = rawrow.invoice_id
    pre_done.append(rows)
invoice_data_processed = pd.DataFrame(pre_done)
invoice_data_processed = invoice_data_processed.fillna('')
for i in invoice_data_processed:
    invoice_data_processed[i]  = invoice_data_processed[i].astype(str)
invoice_data_processed['invoice_id'] = invoice_data_processed['invoice_id'].astype(int)
invoice_data_processed.to_gbq('ALL_SALES.invoices_data', project_id='rising-minutia-372107',chunksize=20000, if_exists='append', credentials=gbq_credential)

q = f'''select * from hb_ref.product  '''
product_description = query_df(q, iterate = False)

for i in product_description:
    product_description[i] = product_description[i].astype(str)
product_description.to_gbq('ALL_SALES.product_description', project_id='rising-minutia-372107',chunksize=20000, if_exists='replace', credentials=gbq_credential)