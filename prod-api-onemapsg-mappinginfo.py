# Databricks notebook source
import pandas as pd
import numpy as np
import janitor
import math
import os
from datetime import datetime
import time
import json
import requests

# for parallel processing
from concurrent.futures import ThreadPoolExecutor, as_completed


# COMMAND ----------

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

df_etl_sf_listing = pd.read_sql(
    '''
    SELECT DISTINCT zip_code, country_zip_code FROM tbl_hc_eelisting
    WHERE country_zip_code LIKE 'Singapore'
    AND location IN ('SINGAPORE')
    AND source = 'SF Direct'
    '''
    ,engine
)
df_etl_sf_listing.info()

# COMMAND ----------

office_loc_map = {
    "Singapore": '819659', # defacto to ICC1 if unclear
    "Inflight Catering Centre 1": '819659',
    "Inflight Catering Centre 2": '499612',
    "Commercial Cookhouse": '',
    "234 Pandan Loop": '128422',
    "AirFreight Terminal 5": '819830',
    "7 Buroh Lane": '618309',
    "SGP (Other Commercial Outlets)": '819659', # defacto to ICC1 if unclear
    "2 Buroh Lane": '618492',
    "SATS Aero Laundry": '509011',
    "212 Pandan Loop": '128403',
    "Seletar Airport": '797405',
    "Marina Bay Cruise Centre SGP": '018947',
    "Terminal 4": '819665',
    "Terminal 3": '819663',
    "9 Buroh Lane": '618309',
    "210 Pandan Loop": '128402',
    "SATS Maintenance Centre": '499614',
    "Terminal 1": '819642',
    "20 Harbour Drive": '117612',
    "AirFreight Terminal 6": '819833',
    "Terminal 2": '819643',
    "AirFreight Terminal 4": '819833',
    "AirFreight Terminal 1": '819833',
    "Taiwan (Overseas Cookhouses)": '819659' # defacto to ICC1 if unclear
}

# COMMAND ----------

def get_token():
    """
    Function to get the token from the API.
    It uses environment variables for email and password for security.
    """
    api_url = "https://www.onemap.gov.sg/api/auth/post/getToken"
    params = {
        "email": os.getenv('ONEMAPSG_EMAIL'),
        "password": os.getenv('ONEMAPSG_PW')
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
    }
    try:
        response = requests.post(url=api_url, headers=headers, json=params)
        response.raise_for_status()  # Raises an exception for HTTP errors
        data = json.loads(response.text)
        dailyToken = data['access_token']
        return dailyToken
    except requests.exceptions.HTTPError as errh:
        print(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")

TOKEN = get_token()
print(TOKEN)
print(type(TOKEN))

# COMMAND ----------

def getRouting(): #test
    api_url = "https://www.onemap.gov.sg/api/public/routingsvc/route"
    params = {
        'start': '1.4346872,103.7754301',
        'end': '1.3412625,103.9848113',
        'routeType': 'pt',
        'date': '10-19-2023',
        'time': '08:00:00',
        'mode': 'TRANSIT',
        'maxWalkDistance': '1000',
        'numItineraries': '1'
    }
    params = {
        'start': '1.4346872,103.7754301',
        #'end': '1.3419335,103.9830591',
        'end': '1.3412625,103.9848113',
        'routeType': 'pt',
        'date': '10-13-2023',
        'time': '07:35:00',
        'mode': 'TRANSIT',
        'maxWalkDistance': '2000',
        'numItineraries': '1'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'Authorization': TOKEN
    }

    response = requests.get(url=api_url, params=params, headers=headers)
    print(response.url)
    print(response.status_code)
    print(response.reason)
    print(response.request.headers)
    print(response.text)
    data = json.loads(response.text)
    #searchfound = data['found']
    #results = data['results']

    #return results

r = getRouting()
r

# COMMAND ----------

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'Authorization': TOKEN
    }
url = "https://www.onemap.gov.sg/api/public/routingsvc/route?start=1.320981%2C103.844150&end=1.326762%2C103.8559&routeType=pt&date=08-13-2023&time=07%3A35%3A00&mode=TRANSIT&maxWalkDistance=1000&numItineraries=3"

response = requests.get(url=url, headers=headers)
print(response.url)
print(response.status_code)
print(response.reason)
print(response.request.headers)

# COMMAND ----------

def load_geo_data(zip_code):
    """
    Function to load geographical data based on the zip code.
    Implements retry logic and error handling.
    """
    if pd.isna(zip_code):
        return ''

    zipcode = str(zip_code + " Singapore")
    api_url = "https://www.onemap.gov.sg/api/common/elastic/search"
    params = {'searchVal': zipcode, 'returnGeom': 'Y', 'getAddrDetails': 'Y'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0'
    }

    NUM_RETRIES = 3
    for i in range(NUM_RETRIES):
        try:
            response = requests.get(url=api_url, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)

            searchfound = data['found']
            results = data['results']
            if searchfound == 0:
                return ''
            else:
                df = pd.json_normalize(results)
                df = (
                    df.clean_names()
                    .convert_dtypes()
                )
                # get the data
                values = f"{df.iloc[0].latitude};{df.iloc[0].longitude};{df.iloc[0].road_name};{df.iloc[0].address}"
                return values
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}, retrying ({i+1}/{NUM_RETRIES})")
            time.sleep(1)

    return ''

# COMMAND ----------

# testing 
load_geo_data('730154')

# COMMAND ----------

def parallel_load_geo_data(zip_codes):
    """
    Function to load geographical data in parallel.
    """
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(load_geo_data, zip_code): zip_code for zip_code in zip_codes}
        results = {}
        for future in as_completed(futures):
            zip_code = futures[future]
            try:
                data = future.result()
                results[zip_code] = data
            except Exception as exc:
                print(f'{zip_code} generated an exception: {exc}')
        return results

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

def get_unique_address_lat_long(df):
    """
    Function to get unique addresses and their lat-long from the DataFrame.
    Utilizes vectorized operations for better performance.
    """
    # Extracting unique zip codes
    unique_zip_codes = df['zip_code'].dropna().unique()
    
    # Getting geographical data in parallel
    geo_data = parallel_load_geo_data(unique_zip_codes)
    
    # Mapping results back to the DataFrame
    df['zipcode_latlong'] = df['zip_code'].map(geo_data)
    address = df[['zip_code', 'zipcode_latlong']].dropna()
    address[['lat', 'long', 'road_name', 'address']] = address['zipcode_latlong'].str.split(';', expand=True)
    address = address.drop('zipcode_latlong', axis=1)
    return address

# COMMAND ----------

df_coordinates = get_unique_address_lat_long(df_etl_sf_listing)

# COMMAND ----------

df_coordinates.info()
df = df_coordinates.dropna(subset='address')
df.info()

# COMMAND ----------

df

# COMMAND ----------

db.insert_with_progress(df,'ref_onemapsg', engine)

# COMMAND ----------


