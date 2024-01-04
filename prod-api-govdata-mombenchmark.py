# Databricks notebook source
import pandas as pd
import numpy as np

from io import StringIO
import json
import requests

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

def getGovData_MOMBenchmark():
    api_url = "https://data.gov.sg/api/action/datastore_search"
    params = {
        'resource_id': 'd_a2fcf3ba95a64a326534bd3e55b5222b',
        'sort': 'quarter desc',
        'filters': '{"occupation1":"total"}'
        # from URL: https://beta.data.gov.sg/collections/683/datasets/d_a2fcf3ba95a64a326534bd3e55b5222b/view
        # Average Monthly Recruitment/Resignation Rates by Industry and Occupational Group, Quarterly
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br'
    }
    response = requests.get(url=api_url, params=params, headers=headers)
    if response.status_code == 200:
        #print(response.text)
        data = json.loads(response.text)
        results = data['result']['records']
        df = pd.json_normalize(results)
        df = df.dropna(axis=1)
        df = cf.strip_clean_drop(df)
        #print(df)
        return df
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        exit()

momData = getGovData_MOMBenchmark()


# COMMAND ----------

momData

# COMMAND ----------

def selectData(df):
    selected_df = df.query(" occupation1=='total' & (industry3=='food, beverages and tobacco' or industry3=='air transport and supporting services' or industry3=='total')").copy()
    selected_df[['year','q']] = selected_df.quarter.str.split("-Q", n=1, expand=True)
    selected_df['year'] = selected_df['year'].astype('int',errors='ignore')
    selected_df['q'] = selected_df['q'].astype('int',errors='ignore')
    selected_df['recruitment_rate'] = selected_df['recruitment_rate'].astype('float',errors='ignore')
    selected_df['resignation_rate'] = selected_df['resignation_rate'].astype('float',errors='ignore')

    selected_df['resignation_rate'] = selected_df['resignation_rate']/100

    selected_df = selected_df.sort_values(by=['year','q'],ascending=False)
    selected_df = selected_df.reset_index(drop=True)
    return selected_df

benchmark_df = selectData(momData)

# COMMAND ----------

benchmark_df.info()

# COMMAND ----------

benchmark_df

# COMMAND ----------

db.insert_with_progress(benchmark_df,"ref_mombenchmark",engine)

# COMMAND ----------


