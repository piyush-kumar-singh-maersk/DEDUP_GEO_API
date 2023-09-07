from fastapi import FastAPI, Path
from typing import Optional
from pydantic import BaseModel
import pandas as pd
import numpy as np
from geopy.distance import lonlat, distance
from fuzzywuzzy import process, fuzz
import json
from fastapi.responses import JSONResponse
from fastapi import Request
import uvicorn
import httpx
import requests
from starlette.requests import Request
import time
app = FastAPI()

#list[str] = []

# #############################################################
# # BULK CODE FOR API

# @app.post('/bulk-dedup-create-record')
# async def get_body(ALL_JSON: list[str], request: Request):
#     JScontent = await request.json()
#     for temp in JScontent["DATA"]:
#         print(json.dumps(JScontent["DATA"][temp]))
#         ALL_JSON = ALL_JSON.append(json.dumps(JScontent["DATA"][temp]))

#         response = httpx.post("http://127.0.0.1:8000/dedup-create-record/", 
#                               data={"RKST": "2", "CITY_NAME": "Jamshedpur", "RW_DESC": "Jharkhand", "COUNTRY_CODE": "IN"}, 
#                               headers={"Content-Type": "application/json"})

#         print(response.json())

#     return JScontent

# #############################################################

@app.post('/bulk-dedup-create-record')
async def get_body(request: Request):
    #ALL_JSON = []
    JScontent = await request.json()
    #print(json.dumps(JScontent))
    # res = requests.post('http://localhost:8000/dedup-create-record/', 
    #                     headers = {'Content-type': 'application/json'}, 
    #                     json = { "RKST": "2", "CITY_NAME": "Jamshedpur", "RW_DESC": "Jharkhand", "COUNTRY_CODE": "IN"})
    
    jsonfile = {}
    counter = 1
    for temp in JScontent["DATA"]:
        #print(json.dumps(JScontent["DATA"][temp]))
        res = requests.post('http://localhost:8000/dedup-create-record/', 
                        headers = {'Content-type': 'application/json'}, 
                        json = JScontent["DATA"][temp])
        temp = {}
        if (res.status_code == 200):
            #STATUS
            status_key = 'Status'
            status_value = res.status_code
            temp[status_key] = status_value
            #JSON
            json_key = 'Response'
            json_value = res.json()
            temp[json_key] = json_value
            #temp.append(JScontent["DATA"][temp])
            #temp.append(str("STATUS : " + str(res.status_code)))
            #temp.append(res.json())
        else:
            #STATUS
            status_key = 'Status'
            status_value = res.status_code
            temp[status_key] = status_value
            #JSON
            json_key = 'Response'
            json_value = None
            temp[json_key] = json_value
            #temp.append(JScontent["DATA"][temp])
            #temp.append(str("STATUS : " + str(res.status_code)))
            #temp.append(None)
        final_key = 'RESPONSE ' + str(counter)
        final_value = temp
        #jsonfile.append(temp)
        jsonfile[final_key] = final_value
        counter += 1
    #df_final = pd.DataFrame(list(zip(requestsfile, statuscode, jsonfile)), columns=['REQUESTS', 'STATUS', 'JSON'])
    return jsonfile

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)

