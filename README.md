### Full Requirments to Run:

Python verion:  Python 3.8.8

#### Libraries:
Full Requirments to Run:

pip install mysql-connector-python

pip install pandas

pip install pandas-gbq

pip install oauth2client

pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

pip install gspread

pip install google-cloud-bigquery


#### Files and what they do:

ga4.py - GA4 API service Class - needed for GA4 parsing

adsense.py - Adsense API Full script - parses API data and loads data to Bigquery

invoice_reloader.py - Full procedure to read data from MySQL on premise and load it onto BigQuery
#### Update Schedule:

adsense.py - Daily
invoice_reloader.py - once per hour




