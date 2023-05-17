# Databricks notebook source
import requests
import pandas as pd
import numpy as np
import json
import janitor
import math
import os
from datetime import datetime

# commonfunc
import pkg_commonfunctions as cf
# connect to db
import pkg_dbconnect as db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

df_etl_sf_listing = pd.read_sql(
    '''
    SELECT DISTINCT zip_code, country_zip_code, office_location FROM tbl_sf_sql_daily
    WHERE country_zip_code LIKE 'Singapore'
    '''
    ,engine
)

# COMMAND ----------

def getToken():
    # get token
    api_url = "https://developers.onemap.sg/privateapi/auth/post/getToken"
    params = {
        "email": os.getenv('ONEMAPSG_EMAIL'),
        "password": os.getenv('ONEMAPSG_PW')
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0'
    }
    response = requests.post(url=api_url, headers=headers, data=params)
    if response.status_code == 200:
        data = json.loads(response.text)
        dailyToken = data['access_token']
        #print(dailyToken)
    else:
        print(f"Error: {response.status_code}")
        exit()

    return dailyToken

TOKEN = getToken()
print(TOKEN)

# COMMAND ----------

def loadGeoData(row):
    if pd.isna(row['zip_code']):
        values = ''
        return values
    else:
        zipcode = str(row['zip_code']+" Singapore")
        api_url = "https://developers.onemap.sg/commonapi/search"
        params = {'searchVal': zipcode, 'returnGeom': 'Y', 'getAddrDetails': 'Y'}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0'
        }

    NUM_RETRIES = 1
    for i in range(NUM_RETRIES):
        try:
            response = requests.get(url=api_url, params=params, headers=headers)
            if response.status_code in [200]:
                ## Escape for loop if returns a successful response
                break
        except:
            print("exceed limit, sleeping")
            time.sleep(1)
            pass
    
    if response is not None and response.status_code == 200:
        data = json.loads(response.text)
        searchfound = data['found']
        results = data['results']
        if searchfound == 0:
            values = ""
            return values
        else:
            df = pd.json_normalize(results)
            df = (
                df.clean_names()
                .convert_dtypes()
            )
            values = f"{df.iloc[0].latitude};{df.iloc[0].longitude};{df.iloc[0].road_name};{df.iloc[0].address}"
        return values
    
    return values

# COMMAND ----------

def getUniqueAddressLatLong(df):
    address = df[['zip_code']]
    address = address.dropna()
    address = address.drop_duplicates()
    #data = pd.DataFrame()
    #data['search'] = df['country_zip_code']+df['zip_code']
    address['zip_code'] = df.zip_code.unique()
    address['zipcode_latlong'] = address.apply(lambda row: loadGeoData(row),axis=1)
    address[['lat','long','road_name','address']] = address.zipcode_latlong.str.split(pat=";",expand=True)
    address = address.drop('zipcode_latlong',axis=1)
    #address['zipcode_latlong'] = address['zipcode_latlong'].str.split(pat=";",expand=True)

    return address

# COMMAND ----------

df_coordinates = getUniqueAddressLatLong(df_etl_sf_listing)
#df_etl_sf = df_etl_sf.merge(df_coordinates,on='zip_code')

# COMMAND ----------

df_coordinates.info()
df = df_coordinates[df_coordinates["zip_code"].str.contains("<NA>") == False]

# COMMAND ----------

df

# COMMAND ----------

db.insert_with_progress(df,'ref_onemapsg', engine)

# COMMAND ----------

df_onemap = pd.read_sql(
    '''
    SELECT * 
    FROM ref_onemapsg
    '''
    ,engine
)
