# Databricks notebook source
import pandas as pd
import numpy as np
import janitor
import math
import os
from datetime import datetime

import xmltodict
import json
import requests
from bs4 import BeautifulSoup


# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

sf_assertion_SAML = "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48c2FtbDI6QXNzZXJ0aW9uIHhtbG5zOnNhbWwyPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6YXNzZXJ0aW9uIiBJRD0iNWFkN2RjNGQtYzQ0My00MjRhLWFlZWYtMWI4MDM4YWI4Y2ZlIiBJc3N1ZUluc3RhbnQ9IjIwMjMtMDctMDNUMDU6MTY6MzguNDQ1WiIgVmVyc2lvbj0iMi4wIiB4bWxuczp4cz0iaHR0cDovL3d3dy53My5vcmcvMjAwMS9YTUxTY2hlbWEiIHhtbG5zOnhzaT0iaHR0cDovL3d3dy53My5vcmcvMjAwMS9YTUxTY2hlbWEtaW5zdGFuY2UiPjxzYW1sMjpJc3N1ZXI+d3d3LnN1Y2Nlc3NmYWN0b3JzLmNvbTwvc2FtbDI6SXNzdWVyPjxkczpTaWduYXR1cmUgeG1sbnM6ZHM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvMDkveG1sZHNpZyMiPjxkczpTaWduZWRJbmZvPjxkczpDYW5vbmljYWxpemF0aW9uTWV0aG9kIEFsZ29yaXRobT0iaHR0cDovL3d3dy53My5vcmcvMjAwMS8xMC94bWwtZXhjLWMxNG4jIi8+PGRzOlNpZ25hdHVyZU1ldGhvZCBBbGdvcml0aG09Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvMDQveG1sZHNpZy1tb3JlI3JzYS1zaGEyNTYiLz48ZHM6UmVmZXJlbmNlIFVSST0iIzVhZDdkYzRkLWM0NDMtNDI0YS1hZWVmLTFiODAzOGFiOGNmZSI+PGRzOlRyYW5zZm9ybXM+PGRzOlRyYW5zZm9ybSBBbGdvcml0aG09Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvMDkveG1sZHNpZyNlbnZlbG9wZWQtc2lnbmF0dXJlIi8+PGRzOlRyYW5zZm9ybSBBbGdvcml0aG09Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvMTAveG1sLWV4Yy1jMTRuIyI+PGVjOkluY2x1c2l2ZU5hbWVzcGFjZXMgeG1sbnM6ZWM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDEvMTAveG1sLWV4Yy1jMTRuIyIgUHJlZml4TGlzdD0ieHMiLz48L2RzOlRyYW5zZm9ybT48L2RzOlRyYW5zZm9ybXM+PGRzOkRpZ2VzdE1ldGhvZCBBbGdvcml0aG09Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvMDkveG1sZHNpZyNzaGExIi8+PGRzOkRpZ2VzdFZhbHVlPmZObTE1dHpnaFdyM25PQm8yMThOWG9XMERKUT08L2RzOkRpZ2VzdFZhbHVlPjwvZHM6UmVmZXJlbmNlPjwvZHM6U2lnbmVkSW5mbz48ZHM6U2lnbmF0dXJlVmFsdWU+WFoybW1qVGFBaXVPd1F1a2tkNVlWc2x3dlJxck4vRzVjSGFNbmNoLy8rVjNIKy9oM3hBQkxPMUYxY3hVMkxremlybGdEZU9xWjVhc3BVSnZKQW5nWlo0NFV4VHdxSERtRjN4cGhOZ3A3ZmlIUmJHR1dJTHErRUEvMG9IMGs1SnlMTU9jbzR6NS9DT2crUjQzaEZOWjIzYzU4Z1kxR3lDR2FSeHJSSHNFVTFNVll2a0IvRzNlSnZQYzJDQW14cW9INUl2R1JEaE9qUFpobTFENk54SVZONXd2VUFjbkIwS2Fub254K1FydTVLeFIrOFFzQ1NiN3JhcHFrSHBwVXFDRllYR3liN1pYYTNxMC9JWVFkQkh2eW1Wc0VZdlRtYTNkaERRS1JFa04zbGFUdDJHZzF6WHdvd1pjWG9ReUVKbXIvbDFsbHV6MlR1ZEs4Y1NFbm4rN1VRPT08L2RzOlNpZ25hdHVyZVZhbHVlPjwvZHM6U2lnbmF0dXJlPjxzYW1sMjpTdWJqZWN0PjxzYW1sMjpOYW1lSUQgRm9ybWF0PSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoxLjE6bmFtZWlkLWZvcm1hdDp1bnNwZWNpZmllZCI+RVkuU0NQLkNZPC9zYW1sMjpOYW1lSUQ+PHNhbWwyOlN1YmplY3RDb25maXJtYXRpb24gTWV0aG9kPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6Y206YmVhcmVyIj48c2FtbDI6U3ViamVjdENvbmZpcm1hdGlvbkRhdGEgTm90T25PckFmdGVyPSIyMDIzLTA3LTEwVDAzOjU2OjM4LjQ0NVoiIFJlY2lwaWVudD0iaHR0cHM6Ly9hcGkxMHByZXZpZXcuc2Fwc2YuY29tL29hdXRoL3Rva2VuIi8+PC9zYW1sMjpTdWJqZWN0Q29uZmlybWF0aW9uPjwvc2FtbDI6U3ViamVjdD48c2FtbDI6Q29uZGl0aW9ucyBOb3RCZWZvcmU9IjIwMjMtMDctMDNUMDU6MDY6MzguNDQ1WiIgTm90T25PckFmdGVyPSIyMDIzLTA3LTEwVDAzOjU2OjM4LjQ0NVoiPjxzYW1sMjpBdWRpZW5jZVJlc3RyaWN0aW9uPjxzYW1sMjpBdWRpZW5jZT53d3cuc3VjY2Vzc2ZhY3RvcnMuY29tPC9zYW1sMjpBdWRpZW5jZT48L3NhbWwyOkF1ZGllbmNlUmVzdHJpY3Rpb24+PC9zYW1sMjpDb25kaXRpb25zPjxzYW1sMjpBdXRoblN0YXRlbWVudCBBdXRobkluc3RhbnQ9IjIwMjMtMDctMDNUMDU6MTY6MzguNDQ1WiIgU2Vzc2lvbkluZGV4PSIxYTZmYjA4Mi0zOTdjLTRjNTMtYWVlOS02NzdhZDY1NGY5ZjMiPjxzYW1sMjpBdXRobkNvbnRleHQ+PHNhbWwyOkF1dGhuQ29udGV4dENsYXNzUmVmPnVybjpvYXNpczpuYW1lczp0YzpTQU1MOjIuMDphYzpjbGFzc2VzOlBhc3N3b3JkUHJvdGVjdGVkVHJhbnNwb3J0PC9zYW1sMjpBdXRobkNvbnRleHRDbGFzc1JlZj48L3NhbWwyOkF1dGhuQ29udGV4dD48L3NhbWwyOkF1dGhuU3RhdGVtZW50PjxzYW1sMjpBdHRyaWJ1dGVTdGF0ZW1lbnQ+PHNhbWwyOkF0dHJpYnV0ZSBOYW1lPSJhcGlfa2V5Ij48c2FtbDI6QXR0cmlidXRlVmFsdWUgeHNpOnR5cGU9InhzOnN0cmluZyI+WWpWak9HTXpPR1V3T1dReE9ETTFOREppTVdJek1Ea3dNVGN6TkE8L3NhbWwyOkF0dHJpYnV0ZVZhbHVlPjwvc2FtbDI6QXR0cmlidXRlPjwvc2FtbDI6QXR0cmlidXRlU3RhdGVtZW50PjxzYW1sMjpBdHRyaWJ1dGVTdGF0ZW1lbnQ+PHNhbWwyOkF0dHJpYnV0ZSBOYW1lPSJ1c2VfdXNlcm5hbWUiPjxzYW1sMjpBdHRyaWJ1dGVWYWx1ZSB4c2k6dHlwZT0ieHM6c3RyaW5nIj50cnVlPC9zYW1sMjpBdHRyaWJ1dGVWYWx1ZT48L3NhbWwyOkF0dHJpYnV0ZT48L3NhbWwyOkF0dHJpYnV0ZVN0YXRlbWVudD48L3NhbWwyOkFzc2VydGlvbj4="
api_key = 'YjVjOGMzOGUwOWQxODM1NDJiMWIzMDkwMTczNA'

# COMMAND ----------

def getToken_SFUAT():
    # get token
    api_url = "https://api10preview.sapsf.com/oauth/token"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    body = {
        'company_id': 'satsltdT1',
        'client_id': api_key,
        'grant_type': 'urn:ietf:params:oauth:grant-type:saml2-bearer',
        'assertion': sf_assertion_SAML
    }
    response = requests.post(url=api_url, headers=headers, data=body)
    if response.status_code == 200:
        data = json.loads(response.text)
        print(data)
        Token = data['access_token']
        print(data)
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        exit()

    return Token

SF_TOKEN = getToken_SFUAT()
auth_header = {'Authorization': 'Bearer ' + SF_TOKEN}

# COMMAND ----------

def getMetadata():
    api2 = "https://api10preview.sapsf.com/odata/v2/EmpEmployment?$filter=personIdExternal eq 'mcolton1'&$expand=empGlobalAssignmentNav&$format=JSON"

    refresh_metadata_url = "https://api10preview.sapsf.com/odata/v2/refreshMetadata"
    api_url = "https://api10preview.sapsf.com/odata/v2/$metadata"
    emp_info_url = "https://api10preview.sapsf.com/odata/v2/EmpEmployment?$format=JSON&$filter=startDate le datetime'2019-09-09T00:00:00' and (endDate eq null or endDate ge datetime'2019-09-09T00:00:00')"
    headers = {
        'Authorization': 'Bearer ' + SF_TOKEN
    }
    response = requests.get(url=api_url, headers=headers)
    if response.status_code == 200:
        print(response.text)
        #soup = BeautifulSoup(response.text, 'xml')
        #data_dict = xmltodict.parse(response.text)
        # write the xml data
        with open("data.xml", "w") as xml_file:
            xml_file.write(response.text)

        json_data = json.dumps(data_dict)
        
        # write the json data
        with open("data.json", "w") as json_file:
            json_file.write(json_data)
            # json_file.close()
        print(json_data)

        #df = pd.json_normalize(json_data)
        #print(df)
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        exit()
    
    #return soup

#md = getMetadata()

# COMMAND ----------

def get_all_pages():
    url = "https://api10preview.sapsf.com/odata/v2/EmpEmployment?$format=JSON&$filter=startDate le datetime'2023-03-31T00:00:00' and (endDate eq null or endDate ge datetime'2023-03-31T00:00:00')&$select=personIdExternal,userId,startDate,endDate"
    all_data = [] # list to store all the data
    
    while url:
        response = requests.get(url, headers=auth_header)
        data = response.json()  # assuming server responds with JSON
        
        all_data.extend(data['d']['results'])  # add the current page's data to our list

        # check if there is a next page
        url = data['d'].get('__next')  # this will return None if '__next' doesn't exist
    
    df = pd.json_normalize(all_data)
    return df
df = get_all_pages()

# COMMAND ----------

df['endDate'] = df['endDate'].apply(cf.convert_unix_timestamp)
df['startDate'] = df['startDate'].apply(cf.convert_unix_timestamp)

# COMMAND ----------

df.query(' userId=="88013518" ')

# COMMAND ----------

def getEmployees():
    emp_info_url = "https://api10preview.sapsf.com/odata/v2/EmpEmployment?$format=JSON&$filter=startDate le datetime'2023-03-31T00:00:00' and (endDate eq null or endDate ge datetime'2023-03-31T00:00:00')&$select=personIdExternal,userId,startDate,endDate"
    auth_header = {'Authorization': 'Bearer ' + SF_TOKEN}
    response = requests.get(url=emp_info_url, headers=auth_header)
    dflist = []
    if response.status_code == 200:
        data = json.loads(response.text)
        df = pd.json_normalize(ee['d']['results'])
        #results = data['results']
        #df = pd.json_normalize(results)
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        exit()
    
    return data

ee = getEmployees()
ee

# COMMAND ----------

ee['d']['__next']

# COMMAND ----------

df = pd.json_normalize(ee['d']['results'])
df

# COMMAND ----------


