#!/usr/bin/env python
# coding: utf-8

# In[18]:


import http.client
import json
import os.path
from datetime import date
import datetime
import time
import sys
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# In[19]:


# !pip install --upgrade google-cloud-bigquery google-cloud-storage


# In[20]:



state_code = int(os.getenv('STATE_CODE_ENV'))
logFolder = os.getenv('LOG_FOLDER_ENV')
# state_code = 20
# logFolder = 'logs'


# In[21]:




# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "bq-sql-cloudstorage.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


# In[22]:


columns = ['district_code', 'state_code', 'timestamp', 'center_id', 'district_name', 'state_name', 'block_name',
               'from','to',
               'fee_type', 'session_date', 'available_capacity', 'min_age_limit', 'vaccine']
schemaDict = {
    'district_code': 'INTEGER', 
    'state_code': 'INTEGER',
    'timestamp': 'TIMESTAMP',
    'center_id': 'INTEGER',
    'district_name': 'STRING',
    'state_name': 'STRING',
    'block_name': 'STRING',
    'from': 'TIME',
    'to': 'TIME',
    'fee_type': 'STRING',
    'session_date': 'TIMESTAMP',
    'available_capacity': 'FLOAT',
    'min_age_limit': 'INTEGER',
    'vaccine': 'STRING'
}
schema = [bigquery.SchemaField(x, schemaDict[x], mode="NULLABLE") for x in columns]
table_id = 'cowin-313417.vaccinelogs.%s%s'%(logFolder,state_code)
table = bigquery.Table(table_id, schema=schema)

table = bigquery_client.create_table(table, exists_ok=True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


# In[23]:


storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
bucket = storage_client.bucket('cowin_logs')


# In[24]:



if not os.path.isdir(logFolder):
    os.mkdir(logFolder)
conn = http.client.HTTPSConnection("cdn-api.co-vin.in")
payload = ''
headers = {}
conn.request("GET", "/api/v2/admin/location/districts/%s"%(state_code), payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))

districtList = json.loads(data.decode('utf8').replace("'", '"'))

districtIdList = [x['district_id'] for x in districtList['districts']]


# In[25]:


def uploadToDatabase(timestamp_now, district_code,state_code, data_str):
#     try:
    pdtime = str(pd.Timestamp(datetime.datetime.fromtimestamp(timestamp_now)))
    
    try:
        data = json.loads(data_str)
    except Exception as e:
        print(e)
        return
    if 'centers' not in data:
        print('centers not in data',data)
        return
    rows = []
    for center in data['centers']:
        center_id = center['center_id']
        district_name = center['district_name']
        state_name = center['state_name']
        block_name = center['block_name']
        fee_type = center['fee_type']
        from_ = center['from']
        to_ = center['to']
        for session in center['sessions']:
            date = str(pd.Timestamp(session['date']))
            available_capacity = session['available_capacity']
            min_age_limit = session['min_age_limit']
            vaccine = session['vaccine']
            row = [district_code, state_code, pdtime, center_id, district_name, state_name, block_name,
                   from_, to_,
                   fee_type,
                   date, available_capacity, min_age_limit, vaccine]
            # data_rows.append(row)
            rows.append(row)

    rows_to_insert = []
    for row in rows:
        rowDict = dict()
        for name, value in zip(columns, row):
            rowDict[name] = value
        rows_to_insert.append(rowDict)
    errors = bigquery_client.insert_rows_json(table_id,rows_to_insert)
    if not errors == []:
        print("Encountered errors while inserting rows: {}".format(errors))
    return
#     except Exception as e:
#         print(e)
#         bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
#         return


# In[26]:


def uploadToStorage(state_code, source_file_name):
#     try:
    destination_blob_name = "%s/%s"%(state_code,fileName)
    blob = bucket.blob(destination_blob_name)
    errors = blob.upload_from_filename(source_file_name)
    if errors is not None:
        print("Encountered errors while uploading logs: {}".format(errors))
    # print(
    #     "File {} uploaded to {}.".format(
    #         source_file_name, destination_blob_name
    #     )
    # )
    os.remove(source_file_name)
    return
#     except Exception as e:
#         print(e)
#         storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
#         bucket = storage_client.bucket('cowin_logs')
        

    


# In[27]:




conn = http.client.HTTPSConnection("cdn-api.co-vin.in")
payload = ''
headers = {}
cache = dict()
fileCachePointer = dict()
for districtId in districtIdList:
    cache["%s_%s" % (state_code, districtId)] = ""
    fileCachePointer["%s_%s" % (state_code, districtId)] = None
while (True):
    for districtId in districtIdList:
        conn.request("GET",
                     "/api/v2/appointment/sessions/public/calendarByDistrict?district_id=%s&date=%s" % (
                         districtId,
                         date.today().strftime("%d-%m-%Y"))
                     , payload, headers)
        res = conn.getresponse()
        if res.status != 200:
            print("[Error]: ", res.status, res.msg.as_string())
            conn.close()
            conn = http.client.HTTPSConnection("cdn-api.co-vin.in")
        data = res.read()
        timestamp_now = datetime.datetime.now().timestamp()
        if data.decode("utf-8") != cache["%s_%s" % (state_code, districtId)]:
            # print(data.decode("utf-8"))
            
            fileName = "%s/%s_%s_%s.txt"%(logFolder,timestamp_now, state_code, districtId)
            f = open(fileName, 'w')
            f.write(data.decode("utf-8"))
            f.close()
            cache["%s_%s" % (state_code, districtId)] = data.decode("utf-8")
            fileCachePointer["%s_%s"%(state_code, districtId)] = fileName
            
        else:
            fileName = "%s/%s_%s_%s.txt"%(logFolder,timestamp_now, state_code, districtId)
            f = open(fileName, 'w')
            f.write(fileCachePointer["%s_%s"%(state_code, districtId)])
            f.close()

        try:
            uploadToStorage(state_code, fileName)
        except Exception as e:
            print(e)
        try:
            uploadToDatabase(timestamp_now, districtId, state_code, data.decode("utf-8"))
        except Exception as e:
            print(e)
        time.sleep(5)


# In[ ]:




