from ga4 import ga4
import datetime

list_dt = []
dt = datetime.date(2022,12,27)
while dt < datetime.datetime.today().date():
    list_dt.append(str(dt))
    dt = dt+ datetime.timedelta(days=1)
    
# simle_load

ga = ga4('344897288')
for i in list_dt:
    dataframe_loaded = ga.report_get(
        dimetions = ['pageReferrer', 'pagePath'], 
        metrics = ['screenPageViews'],
        dates= [i,i])
    
# filter_loaded
ga = ga4('344860207')
for i in list_dt[:2]:
    filter_dataframe_loaded = ga.report_get(
        dimetions = ['pageReferrer', 'pagePath'], 
        metrics = ['screenPageViews'],
        filters = {'pagePath':'/Brookline+MA'},
        dates= [i,i])