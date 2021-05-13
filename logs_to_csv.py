import http.client
import json
from datetime import date
import datetime
import time
import csv
import pandas as pd
import numpy as np
import glob
import os
import sys

if __name__=='__main__':

    columns = ['district_code', 'state_code', 'timestamp', 'center_id', 'district_name', 'state_name', 'block_name',
               'from','to',
               'fee_type', 'session_date', 'available_capacity', 'min_age_limit', 'vaccine']
    # data_rows = []
    logFolder = sys.argv[1]
    glob_results = glob.glob('%s/*.txt'%(logFolder))
    glob_results.sort()

    csvfile = open(sys.argv[2], 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(columns)

    invalidfiles = 0

    for file in glob_results:
        timestamp, state_code, district_code = os.path.basename(file).replace('.txt', '').split('_')
        f = open(file)
        content = f.read()
        f.close()
        if content[:3] == 'log':
            f = open(content)
            content = f.read()
            f.close()
        pdtime = pd.Timestamp(datetime.datetime.fromtimestamp(float(timestamp)))
        try:
            data = json.loads(content)
        except Exception as e:
            print(e)
            invalidfiles += 1
            continue
        if 'centers' not in data:
            invalidfiles += 1

            continue
        for center in data['centers']:
            center_id = center['center_id']
            district_name = center['district_name']
            state_name = center['state_name']
            block_name = center['block_name']
            fee_type = center['fee_type']
            from_ = center['from']
            to_ = center['to']
            for session in center['sessions']:
                date = pd.Timestamp(session['date'])
                available_capacity = session['available_capacity']
                min_age_limit = session['min_age_limit']
                vaccine = session['vaccine']
                row = [district_code, state_code, pdtime, center_id, district_name, state_name, block_name,
                       from_, to_,
                       fee_type,
                       date, available_capacity, min_age_limit, vaccine]
                # data_rows.append(row)
                csvwriter.writerow(row)
    csvfile.close()
    # df = pd.DataFrame(data_rows, columns=columns)

    # df.to_csv(sys.argv[2],index=False)
