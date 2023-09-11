from fastapi import FastAPI, Path
from typing import Optional
from pydantic import BaseModel
import pandas as pd
import numpy as np
from geopy.distance import lonlat, distance
from fuzzywuzzy import process, fuzz

app = FastAPI()

class City(BaseModel):
    RKST: Optional[str] = None
    CITY_NAME: str
    ALT_CITY_NAME: Optional[str] = None
    RW_DESC: Optional[str] = None
    COUNTRY_CODE: str
    LAT: Optional[str] = None
    LONG: Optional[str] = None

class AltCity(BaseModel):
    RKST: Optional[str] = None
    CITY_NAME: str
    ALT_CITY_NAME: Optional[str] = None
    RW_DESC: Optional[str] = None
    COUNTRY_CODE: str
    LAT: Optional[str] = None
    LONG: Optional[str] = None

class Latlong(BaseModel):
    RKST: Optional[str] = None
    CITY_NAME: str
    ALT_CITY_NAME: Optional[str] = None
    RW_DESC: Optional[str] = None
    COUNTRY_CODE: str
    LAT: str
    LONG: str

class InputAltCity(BaseModel):
    RKST: Optional[str] = None
    CITY_NAME: Optional[str] = None
    ALT_CITY_NAME: str
    RW_DESC: Optional[str] = None
    COUNTRY_CODE: str
    LAT: Optional[str] = None
    LONG: Optional[str] = None

class UnlocCity(BaseModel):
    RKST: Optional[str] = None
    CITY_NAME: str
    ALT_CITY_NAME: Optional[str] = None
    RW_DESC: Optional[str] = None
    COUNTRY_CODE: str
    LAT: Optional[str] = None
    LONG: Optional[str] = None

@app.post("/dedup-iteration-city=city")
def read_root(city_input : City):
    if city_input.RKST is not None:
        value_RKST = city_input.RKST.lower()
    value_CITY_NAME = city_input.CITY_NAME.lower()
    if city_input.ALT_CITY_NAME is not None:
        value_ALT_CITY_NAME = city_input.ALT_CITY_NAME.lower()
    if city_input.RW_DESC is not None and city_input.RW_DESC != '':
        value_RW_DESC = city_input.RW_DESC.lower()
    else:
        value_RW_DESC = 'nan'
    value_COUNTRY_CODE = city_input.COUNTRY_CODE.lower()
    if city_input.LAT is not None:
        value_LAT = city_input.LAT.lower()
    if city_input.LONG is not None:
        value_LONG = city_input.LONG.lower()

    df_smds_data = pd.read_csv("smds_data.csv")
    df_smds_data = df_smds_data.applymap(str)

    temp_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                               (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]

    temp_df = temp_df.reset_index()
    temp_df = temp_df.astype(str)
    temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
    len_temp_data = len(temp_df)

    curr_city_info = value_CITY_NAME

    high_score = 0
    high_score_index = 0
    high_score_address = ''
    high_score_alt_city = ''
    high_score_lat = ''
    high_score_long = ''

    for j in range(0, len_temp_data):
        all_city_info = str(temp_df['CITY_NAME'][j])
        if (curr_city_info == all_city_info):
            return {"Message" : "DUPLICATE - CITY NAME MATCH", 
                    "Data" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                              "DUP_MATCH_UNLOC" : temp_df['UNLOC_CODE'][j].replace("nan", "null"),
                              "DUP_MATCH_CITY" : str(all_city_info),
                              "DUP_MATCH_ALT_CITY" : str(temp_df['ALIAS_CITY'][j]).replace("nan", "null"),
                              "DUP_MATCH_SCORE" : str(100),
                              "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                              "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}

    for j in range(0, len_temp_data):
        all_city_info = str(temp_df['CITY_NAME'][j])
        if (fuzz.token_set_ratio(curr_city_info, all_city_info) == 100 and curr_city_info != all_city_info):
            curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info) - 3
        else:
            curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info)
            
        if curr_score >= 90 and curr_score > high_score:
            high_score = curr_score
            high_score_index = j
            high_score_address = str(temp_df['CITY_NAME'][j])
            high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
            high_score_lat = str(temp_df['LAT'][j])
            high_score_long = str(temp_df['LONG'][j])
    
    if high_score >= 90:
        DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
        DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
        DUP_MATCH_CITY = high_score_address
        DUP_MATCH_ALT_CITY = high_score_alt_city
        DUP_MATCH_SCORE = str(high_score)
        DUP_MATCH_LAT = high_score_lat
        DUP_MATCH_LONG = high_score_long
    else:
        DUP_MATCH_RKST = 'null'
        DUP_MATCH_UNLOC = 'null'
        DUP_MATCH_CITY = 'null'
        DUP_MATCH_ALT_CITY = 'null'
        DUP_MATCH_SCORE = 'null'
        DUP_MATCH_LAT = 'null'
        DUP_MATCH_LONG = 'null'

    if DUP_MATCH_CITY != 'null':
        return {"Message" : "DUPLICATE - CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                "Data" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                          "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"),
                          "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                          "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                          "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                          "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                          "DUP_MATCH_LONG" : DUP_MATCH_LONG}}
    else:
        #ALSO ADD THE SAME RECORD TO CURRENT SMDS DB
        return {"Message" : "NOT DUPLICATE"}
    
@app.post("/dedup-iteration-city=alt-city")
def read_root(alt_city_input : AltCity):
    if alt_city_input.RKST is not None:
        value_RKST = alt_city_input.RKST.lower()
    value_CITY_NAME = alt_city_input.CITY_NAME.lower()
    if alt_city_input.ALT_CITY_NAME is not None:
        value_ALT_CITY_NAME = alt_city_input.ALT_CITY_NAME.lower()
    if alt_city_input.RW_DESC is not None and alt_city_input.RW_DESC != '':
        value_RW_DESC = alt_city_input.RW_DESC.lower()
    else:
        value_RW_DESC = 'nan'
    value_COUNTRY_CODE = alt_city_input.COUNTRY_CODE.lower()
    if alt_city_input.LAT is not None:
        value_LAT = alt_city_input.LAT.lower()
    if alt_city_input.LONG is not None:
        value_LONG = alt_city_input.LONG.lower()

    df_smds_data = pd.read_csv("smds_data.csv")
    df_smds_data = df_smds_data.applymap(str)

    temp_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE) & 
                               (df_smds_data['RW_DESC'] == value_RW_DESC)]
    
    temp_df = temp_df.reset_index()
    temp_df = temp_df.astype(str)
    temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
    len_temp_data = len(temp_df)

    curr_city_info = value_CITY_NAME

    high_score = 0
    high_score_index = 0
    high_score_address = ''
    high_score_alt_city = ''
    high_score_lat = ''
    high_score_long = ''

    for j in range(0, len_temp_data):
        all_city_info = str(temp_df['ALIAS_CITY'][j])
        if (curr_city_info == all_city_info and temp_df['ALIAS_CITY'][j] != 'nan'):
            return {"Message" : "DUPLICATE - ALT CITY NAME MATCH", 
                    "Data" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                              "DUP_MATCH_UNLOC" : temp_df['UNLOC_CODE'][j].replace("nan", "null"),
                              "DUP_MATCH_CITY" : str(temp_df['CITY_NAME'][j]),
                              "DUP_MATCH_ALT_CITY" : str(all_city_info).replace("nan", "null"),
                              "DUP_MATCH_SCORE" : str(100),
                              "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                              "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}
    
    for j in range(0, len_temp_data):
        all_city_info = str(temp_df['ALIAS_CITY'][j])
        if (fuzz.token_set_ratio(curr_city_info, all_city_info) == 100 and 
            curr_city_info != all_city_info):
            curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info) - 3
        else:
            curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info)

        if curr_score >= 90 and curr_score > high_score and temp_df['ALIAS_CITY'][j] != 'nan':
            high_score = curr_score
            high_score_index = j
            high_score_address = str(temp_df['CITY_NAME'][j])
            high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
            high_score_lat = str(temp_df['LAT'][j])
            high_score_long = str(temp_df['LONG'][j])
    
    if high_score >= 90:
        DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
        DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
        DUP_MATCH_CITY = high_score_address
        DUP_MATCH_ALT_CITY = high_score_alt_city
        DUP_MATCH_SCORE = str(high_score)
        DUP_MATCH_LAT = high_score_lat
        DUP_MATCH_LONG = high_score_long
    else:
        DUP_MATCH_RKST = 'null'
        DUP_MATCH_UNLOC = 'null'
        DUP_MATCH_CITY = 'null'
        DUP_MATCH_ALT_CITY = 'null'
        DUP_MATCH_SCORE = 'null'
        DUP_MATCH_LAT = 'null'
        DUP_MATCH_LONG = 'null'

    if DUP_MATCH_CITY != 'null':
        return {"Message" : "DUPLICATE - ALT CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                "Data" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                          "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"), 
                          "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                          "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                          "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                          "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                          "DUP_MATCH_LONG" : DUP_MATCH_LONG}}
    else:
        #ALSO ADD THE SAME RECORD TO CURRENT SMDS DB
        return {"Message" : "NOT DUPLICATE"}

@app.post("/dedup-iteration-lat-long=lat-long")
def read_root(lat_long_input : Latlong):
    if lat_long_input.RKST is not None:
        value_RKST = lat_long_input.RKST.lower()
    value_CITY_NAME = lat_long_input.CITY_NAME.lower()
    if lat_long_input.ALT_CITY_NAME is not None:
        value_ALT_CITY_NAME = lat_long_input.ALT_CITY_NAME.lower()
    if lat_long_input.RW_DESC is not None and lat_long_input.RW_DESC != '':
        value_RW_DESC = lat_long_input.RW_DESC.lower()
    else:
        value_RW_DESC = 'nan'
    value_COUNTRY_CODE = lat_long_input.COUNTRY_CODE.lower()
    value_LAT = lat_long_input.LAT.lower()
    value_LONG = lat_long_input.LONG.lower()

    df_smds_data = pd.read_csv("../smds_data.csv")
    df_smds_data = df_smds_data.applymap(str)

    high_score = 9999999999
    high_score_index = 0
    high_score_address = ''
    high_score_alt_city = ''
    high_score_lat = ''
    high_score_long = ''

    # temp_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE) & 
    #                            (df_smds_data['RW_DESC'] == value_RW_DESC)]
    if (value_COUNTRY_CODE in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 'in', 'it', 
                                   'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
            temp_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                                       (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
    else:
            temp_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
    temp_df = temp_df.reset_index()
    temp_df = temp_df.astype(str)
    temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
    len_temp_data = len(temp_df)

    lat_long_info_1 = (value_LONG, value_LAT)

    for j in range(0, len_temp_data):
        lat_long_info_2 = (temp_df['LONG'][j], temp_df['LAT'][j])
        try:
            curr_score = distance(lonlat(*lat_long_info_1), lonlat(*lat_long_info_2)).km
        except:
            curr_score = 999
        if (curr_score <= 1.50000 and curr_score < high_score):
            high_score = curr_score
            high_score_index = j
            high_score_address = str(temp_df['CITY_NAME'][j])
            high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
            high_score_lat = str(temp_df['LAT'][j])
            high_score_long = str(temp_df['LONG'][j])

    if high_score <= 1.50000:
        DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
        DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
        DUP_MATCH_CITY = high_score_address
        DUP_MATCH_ALT_CITY = high_score_alt_city
        DUP_MATCH_SCORE = str(high_score)
        DUP_MATCH_LAT = high_score_lat
        DUP_MATCH_LONG = high_score_long
    else:
        DUP_MATCH_RKST = 'null'
        DUP_MATCH_UNLOC = 'null'
        DUP_MATCH_CITY = 'null'
        DUP_MATCH_ALT_CITY = 'null'
        DUP_MATCH_SCORE = 'null'
        DUP_MATCH_LAT = 'null'
        DUP_MATCH_LONG = 'null'

    if DUP_MATCH_CITY != 'null':
        return {"Message" : "DUPLICATE - LAT LONG MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                    "ValidationStatus" : "FAILED",
                    "InputData" : lat_long_input,
                    "MatchData" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                                   "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"),
                                   "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                                   "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                                   "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                                   "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                                   "DUP_MATCH_LONG" : DUP_MATCH_LONG}}
    else:
        return {"Message" : "NOT DUPLICATE"}

@app.post("/dedup-iteration-alt-city=city+alt-city")
def read_root(altcity_input : InputAltCity):
    if altcity_input.RKST is not None:
        value_RKST = altcity_input.RKST.lower()
    if altcity_input.CITY_NAME is not None:
        value_CITY_NAME = altcity_input.CITY_NAME.lower()
    value_ALT_CITY_NAME = altcity_input.ALT_CITY_NAME.lower()
    if altcity_input.RW_DESC is not None and altcity_input.RW_DESC != '':
        value_RW_DESC = altcity_input.RW_DESC.lower()
    else:
        value_RW_DESC = 'nan'
    value_COUNTRY_CODE = altcity_input.COUNTRY_CODE.lower()
    if altcity_input.LAT is not None:
        value_LAT = altcity_input.LAT.lower()
    if altcity_input.LONG is not None:
        value_LONG = altcity_input.LONG.lower()

    df_smds_data = pd.read_csv("smds_data.csv")
    df_smds_data = df_smds_data.applymap(str)

    temp_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                               (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
    
    temp_df = temp_df.reset_index()
    temp_df = temp_df.astype(str)
    temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
    len_temp_data = len(temp_df)

    curr_city_info = value_ALT_CITY_NAME

    ### CHECK IN MAIN CITY DATA THEN IN ALT CITY DATA ###

    high_score = 0
    high_score_index = 0
    high_score_address = ''
    high_score_alt_city = ''
    high_score_lat = ''
    high_score_long = ''

    for j in range(0, len_temp_data):
        all_city_info_main = str(temp_df['CITY_NAME'][j])
        all_city_info_alt = str(temp_df['ALIAS_CITY'][j])
        if (curr_city_info == all_city_info_main):
            return {"Message" : "DUPLICATE - CITY NAME MATCH", 
                    "Data" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                              "DUP_MATCH_UNLOC" : temp_df['UNLOC_CODE'][j].replace("nan", "null"),
                              "DUP_MATCH_CITY" : str(all_city_info_main),
                              "DUP_MATCH_ALT_CITY" : str(temp_df['ALIAS_CITY'][j]).replace("nan", "null"),
                              "DUP_MATCH_SCORE" : str(100),
                              "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                              "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}
        elif (curr_city_info == all_city_info_alt and temp_df['ALIAS_CITY'][j] != 'nan'):
            return {"Message" : "DUPLICATE - ALT CITY NAME MATCH", 
                    "Data" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                              "DUP_MATCH_UNLOC" : temp_df['UNLOC_CODE'][j].replace("nan", "null"),
                              "DUP_MATCH_CITY" : str(all_city_info_main),
                              "DUP_MATCH_ALT_CITY" : str(temp_df['ALIAS_CITY'][j]).replace("nan", "null"),
                              "DUP_MATCH_SCORE" : str(100),
                              "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                              "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}
        
    for j in range(0, len_temp_data):
        all_city_info_main = str(temp_df['CITY_NAME'][j])
        if (fuzz.token_set_ratio(curr_city_info, all_city_info_main) == 100 and 
            curr_city_info != all_city_info_main):
            curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_main) - 3
        else:
            curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_main)
        if curr_score >= 90 and curr_score > high_score:
            high_score = curr_score
            high_score_index = j
            high_score_address = str(temp_df['CITY_NAME'][j])
            high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
            high_score_lat = str(temp_df['LAT'][j])
            high_score_long = str(temp_df['LONG'][j])
    if high_score >= 90:
        DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
        DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
        DUP_MATCH_CITY = high_score_address
        DUP_MATCH_ALT_CITY = high_score_alt_city
        DUP_MATCH_SCORE = str(high_score)
        DUP_MATCH_LAT = high_score_lat
        DUP_MATCH_LONG = high_score_long
    else:
        DUP_MATCH_RKST = 'null'
        DUP_MATCH_UNLOC = 'null'
        DUP_MATCH_CITY = 'null'
        DUP_MATCH_ALT_CITY = 'null'
        DUP_MATCH_SCORE = 'null'
        DUP_MATCH_LAT = 'null'
        DUP_MATCH_LONG = 'null'

    if DUP_MATCH_CITY != 'null':
        return {"Message" : "DUPLICATE - CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                "Data" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                          "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"),
                          "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                          "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                          "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                          "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                          "DUP_MATCH_LONG" : DUP_MATCH_LONG}}
    else:
        for j in range(0, len_temp_data):
            all_city_info_alt = str(temp_df['ALIAS_CITY'][j])
            if (fuzz.token_set_ratio(curr_city_info, all_city_info_alt) == 100 and 
                curr_city_info != all_city_info_alt):
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_alt) - 3
            else:
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_alt)
            if curr_score >= 90 and curr_score > high_score:
                high_score = curr_score
                high_score_index = j
                high_score_address = str(temp_df['CITY_NAME'][j])
                high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
                high_score_lat = str(temp_df['LAT'][j])
                high_score_long = str(temp_df['LONG'][j])
        if high_score >= 90:
            DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
            DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
            DUP_MATCH_CITY = high_score_address
            DUP_MATCH_ALT_CITY = high_score_alt_city
            DUP_MATCH_SCORE = str(high_score)
            DUP_MATCH_LAT = high_score_lat
            DUP_MATCH_LONG = high_score_long
        else:
            DUP_MATCH_RKST = 'null'
            DUP_MATCH_UNLOC = 'null'
            DUP_MATCH_CITY = 'null'
            DUP_MATCH_ALT_CITY = 'null'
            DUP_MATCH_SCORE = 'null'
            DUP_MATCH_LAT = 'null'
            DUP_MATCH_LONG = 'null'

        if DUP_MATCH_CITY != 'null':
            return {"Message" : "DUPLICATE - ALT CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                    "Data" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                              "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"), 
                              "DUP_MATCH_CITY" : DUP_MATCH_CITY,
                              "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                              "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                              "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                              "DUP_MATCH_LONG" : DUP_MATCH_LONG}}
        else:
            return {"Message" : "NOT DUPLICATE"}

@app.post("/dedup-iteration-unloc=unloc")
def read_root(unloc_input : UnlocCity):
    if unloc_input.RKST is not None:
        value_RKST = unloc_input.RKST.lower()
    value_CITY_NAME = unloc_input.CITY_NAME.lower()
    if unloc_input.ALT_CITY_NAME is not None:
        value_ALT_CITY_NAME = unloc_input.ALT_CITY_NAME.lower()
    if unloc_input.RW_DESC is not None and unloc_input.RW_DESC != '':
        value_RW_DESC = unloc_input.RW_DESC.lower()
    else:
        value_RW_DESC = 'nan'
    value_COUNTRY_CODE = unloc_input.COUNTRY_CODE.lower()
    if unloc_input.LAT is not None:
        value_LAT = unloc_input.LAT.lower()
    if unloc_input.LONG is not None:
        value_LONG = unloc_input.LONG.lower()

    df_unloc_data = pd.read_csv("unloc_data.csv")
    df_unloc_data = df_unloc_data.applymap(str)

    temp_df = df_unloc_data.loc[(df_unloc_data['Country'] == value_COUNTRY_CODE) & 
                                (df_unloc_data['State Name'] == value_RW_DESC)]
    temp_df = temp_df.reset_index()
    len_temp_data = len(temp_df)

    unloc_code_found = ''

    for i in range(0, len_temp_data):
        input_combined = str(value_CITY_NAME + '_' + 
                             value_RW_DESC + '_' + 
                             value_COUNTRY_CODE)

        unloc_combined = str(temp_df['Name'][i] + '_' + 
                             temp_df['State Name'][i] + '_' + 
                             temp_df['Country'][i])
        if (input_combined == unloc_combined):
            unloc_code_found = temp_df['UNLOC'][i]
            break

    df_smds_data = pd.read_csv("smds_data.csv")
    df_smds_data = df_smds_data.applymap(str)
    temp_smds_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE) & 
                                (df_smds_data['RW_DESC'] == value_RW_DESC)]
    temp_smds_df = temp_smds_df.reset_index()
    len_temp_smds_df = len(temp_smds_df)
    
    for i in range(0, len_temp_smds_df):
        if (unloc_code_found == temp_smds_df['UNLOC_CODE'][i]):
            return {"Message" : "DUPLICATE - UNLOC MATCH - MATCHED WITH RKST = " + str(temp_smds_df['RKST'][i]).upper(),
                "Data" : {"DUP_MATCH_RKST" : temp_smds_df['RKST'][i],
                          "DUP_MATCH_UNLOC" : temp_smds_df['UNLOC_CODE'][i].replace("nan", "null"),
                          "DUP_MATCH_CITY" : temp_smds_df['CITY_NAME'][i], 
                          "DUP_MATCH_ALT_CITY" : temp_smds_df['ALIAS_CITY'][i].replace("nan", "null"),
                          "DUP_MATCH_SCORE" : 'null', 
                          "DUP_MATCH_LAT" : temp_smds_df['LAT'][i],
                          "DUP_MATCH_LONG" : temp_smds_df['LONG'][i]}}
    
    else:
            return {"Message" : "NOT DUPLICATE"}
