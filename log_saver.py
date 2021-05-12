import http.client
import json
import os.path
from datetime import date
import datetime
import time
import sys



if __name__=="__main__":
    state_code = int(sys.argv[1])
    logFolder = sys.argv[2]
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
            if data.decode("utf-8") != cache["%s_%s" % (state_code, districtId)]:
                # print(data.decode("utf-8"))
                fileName = "%s/%s_%s_%s.txt"%(logFolder,datetime.datetime.now().timestamp(), state_code, districtId)
                f = open(fileName, 'w')
                f.write(data.decode("utf-8"))
                f.close()
                cache["%s_%s" % (state_code, districtId)] = data.decode("utf-8")
                fileCachePointer["%s_%s"%(state_code, districtId)] = fileName
            else:
                fileName = "%s/%s_%s_%s.txt"%(logFolder,datetime.datetime.now().timestamp(), state_code, districtId)
                f = open(fileName, 'w')
                f.write(fileCachePointer["%s_%s"%(state_code, districtId)])
                f.close()
            time.sleep(5)
