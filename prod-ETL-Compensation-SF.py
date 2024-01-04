# Databricks notebook source
# essential imports
import sys
import os
import glob
from datetime import datetime, date
import pandas as pd
import numpy as np
import re
import time

from dateutil.relativedelta import relativedelta

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-cb-info")

# COMMAND ----------

def readComp():
    dflist = []
    filenames = "*compensation*.csv"
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"{file}, {fileCreationDate}")

        try:
            datafile = pd.read_csv(file, header=1, low_memory=False)
        except:
            print(f"Reading {file} failed.")
            pass
        datafile = cf.strip_clean_drop(datafile)
        # set dtypes correctly
        datafile["effective_start_date"] = pd.to_datetime(datafile["effective_start_date"], dayfirst=True, errors="coerce")
        datafile["todays_date"] = datafile['todays_date'].apply(cf.parse_date)
        datafile['persno'] = datafile['persno'].astype('string')
        datafile['total_monthly_basic'] = datafile['total_monthly_basic'].astype('float')

        # remove redundant data
        datafile = datafile.dropna(subset=['total_monthly_basic'])
        datafile = datafile[datafile.total_monthly_basic != 0]
        datafile = datafile[datafile.frequency_name == 'Monthly']
        
        # clean duplicated, keep only if it's the latest effective date
        datafile = datafile.sort_values(by=['persno','effective_start_date'],ascending=True)
        datafile = datafile.drop_duplicates(subset=['persno'],keep='last')

        #datafile = datafile.drop_duplicates(keep='first')
        #datafile = datafile.drop_duplicates(subset=['persno', 'total_monthly_basic'], keep='first')
        dflist.append(datafile)
    
    df = pd.concat(dflist, ignore_index=True)
    df['eom'] = pd.to_datetime(df['todays_date']) + pd.offsets.MonthEnd(0)

    df.info()
    return df

comp_df = readComp()

# COMMAND ----------

def readMercerJobMap():
    filename = f"{root_dir}/HC_SATSGrade_Mercer_Map.xlsx"
    
    try:
        df_jobmap = pd.read_excel(filename, sheet_name=0, header=0)
    except:
        print(f"Reading {filename} failed.")
        pass
    df_jobmap = cf.strip_clean_drop(df_jobmap)

    return df_jobmap

jobmap_df = readMercerJobMap()
jobmap_df.info()

# COMMAND ----------

def readMercerJobFamMap():
    filename = f"{root_dir}/HC_SATSGrade_Mercer_Map.xlsx"
    
    try:
        df_jobfammap = pd.read_excel(filename, sheet_name=1, header=0)
    except:
        print(f"Reading {filename} failed.")
        pass
    df_jobfammap = cf.strip_clean_drop(df_jobfammap)

    return df_jobfammap

jobfammap_df = readMercerJobFamMap()
jobfammap_df.info()

# COMMAND ----------

def readSATSSalRangeMap():
    filename = f"{root_dir}/HC_SATSGrade_Mercer_Map.xlsx"
    
    try:
        df = pd.read_excel(filename, sheet_name=2, header=0)
    except:
        print(f"Reading {filename} failed.")
        pass
    df = cf.strip_clean_drop(df)
    df['business'] = df['business'].str.upper()
    df['business_unit'] = df['business_unit'].str.upper()
    df['salary_range'] = df['salary_range'].str.upper()
    return df

salrangemap_df = readSATSSalRangeMap()
salrangemap_df.info()

# COMMAND ----------

def readSATSSalRange():
    filename = f"{root_dir}/HC_SATSGrade_Mercer_Map.xlsx"
    
    try:
        df = pd.read_excel(filename, sheet_name=3, header=0)
    except:
        print(f"Reading {filename} failed.")
        pass
    df = cf.strip_clean_drop(df)
    df['salary_range'] = df['salary_range'].str.upper()

    return df

salrange_df = readSATSSalRange()
salrange_df.info()

# COMMAND ----------

salary_range = pd.merge(salrangemap_df,salrange_df,how='outer',on='salary_range')

# COMMAND ----------

salary_range.head(20)

# COMMAND ----------

'''def readMercerData():
    filename = f"{root_dir}/MercerData.xlsx"
    
    try:
        df_mercerdata = pd.read_excel(filename, sheet_name=4, header=1)
    except:
        print(f"Reading {filename} failed.")
        pass
    df_mercerdata = cf.strip_clean_drop(df_mercerdata)

    return df_mercerdata

#mercerdata_df = readMercerData()
#mercerdata_df.info()'''

def readMercerData():
    dflist = []
    filenames = "MercerData*.xlsx"
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"reading {file}, {fileCreationDate}")
        try:
            datafile = pd.read_excel(file, sheet_name=4, header=1)
        except:
            print(f"Reading {file} failed.")
            pass
        datafile = cf.strip_clean_drop(datafile)
        datafile['filename'] = file
        datafile = datafile.reset_index(drop=True)

        #pd.DataFrame(list(datafile.columns),columns=['column names']).to_csv(f'columns_{os.path.basename(file)}.csv',index=False)
        dflist.append(datafile)
    
    df = pd.concat(dflist, ignore_index=True)
    df.info()
    return df

mercerdata_df = readMercerData()

# COMMAND ----------

d_mask = [
    'market_view','job_title','job_type','job_code','position_class',
    'base_salary_perc25_iw','base_salary_median_iw','base_salary_perc75_iw', #C1
    'total_guaranteed_cash_comp_perc25_iw','total_guaranteed_cash_comp_median_iw','total_guaranteed_cash_comp_perc75_iw', #C2
    'total_cash_target_perc25_iw','total_cash_target_median_iw','total_cash_target_perc75_iw', #C3
    'tdc_target_b_s_perc25_iw','tdc_target_b_s_median_iw','tdc_target_b_s_perc75_iw' #C4
    ]

md_df = mercerdata_df[d_mask].copy() # get only the columns that we need
# process the data!
md_df[['position','job_level']] = md_df.job_title.str.split(" - ", n=1, expand=True) # mercer data include this dash to split out the job 
md_df['job_level_code'] = md_df.job_code.str.split('.').str[-1] # get the last 3 characters of the job code to get the job level code
md_df['job_family_code'] = md_df.job_code.str.split('.').str[0:2].str.join('.') # get the first 3 characters of the job code to get the job level code


# COMMAND ----------

# melt columns into rows

# list of columns to be converted
cols_c1 = ['base_salary_perc25_iw', 'base_salary_median_iw', 'base_salary_perc75_iw']
cols_c2 = ['total_guaranteed_cash_comp_perc25_iw', 'total_guaranteed_cash_comp_median_iw', 'total_guaranteed_cash_comp_perc75_iw']
cols_c3 = ['total_cash_target_perc25_iw', 'total_cash_target_median_iw', 'total_cash_target_perc75_iw']
cols_c4 = ['tdc_target_b_s_perc25_iw', 'tdc_target_b_s_median_iw', 'tdc_target_b_s_perc75_iw']

# columns that will not be melted
id_vars = ['market_view','job_title', 'job_type', 'job_code', 'position_class', 'position', 'job_level', 'job_level_code','job_family_code']

# melt the dataframe multiple times and assign new column names
df1 = md_df.melt(id_vars=id_vars, value_vars=cols_c1, var_name='var', value_name='value').assign(measure='c1')
df2 = md_df.melt(id_vars=id_vars, value_vars=cols_c2, var_name='var', value_name='value').assign(measure='c2')
df3 = md_df.melt(id_vars=id_vars, value_vars=cols_c3, var_name='var', value_name='value').assign(measure='c3')
df4 = md_df.melt(id_vars=id_vars, value_vars=cols_c4, var_name='var', value_name='value').assign(measure='c4')

# concatenate melted dataframes
md_df_long = pd.concat([df1, df2, df3, df4], ignore_index=True)


# COMMAND ----------

md_df_long.info()

# COMMAND ----------

md_df_long.sample(10)

# COMMAND ----------

display(md_df_long.query('position == "Accounting" and var == "base_salary_perc25_iw"'))

# COMMAND ----------

md_df_long.query('position == "Accounting" and var == "base_salary_perc25_iw"').value.mean()

# COMMAND ----------

display(md_df[md_df.position == "Accounting" ])

# COMMAND ----------

md_df[md_df.position == "Accounting"].base_salary_perc25_iw.sum()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convert the 'value' column to numeric, if it's not already.
md_df_long['value'] = pd.to_numeric(md_df_long['value'], errors='coerce')

plt.figure(figsize=(10, 6))
sns.boxplot(x='measure', y='value', data=md_df_long)
plt.title('Boxplot for each measure')
plt.ylabel('Value')
plt.show()


# COMMAND ----------

comp_df[comp_df.duplicated(subset=['persno','todays_date'],keep=False)].sort_values(by='persno')

# COMMAND ----------

comp_df.query('persno=="88011914"')

# COMMAND ----------

db.insert_with_progress(comp_df,"tbl_sf_sql_comp",engine)

# COMMAND ----------

db.insert_with_progress(md_df_long,"tbl_mercerdata",engine)

# COMMAND ----------

db.insert_with_progress(jobmap_df,"tbl_jobmap",engine)


# COMMAND ----------

db.insert_with_progress(jobfammap_df,"tbl_jobfammap",engine)


# COMMAND ----------

db.insert_with_progress(salary_range,"tbl_salary_range",engine)


# COMMAND ----------

 db.insert_with_progress(salrangemap_df,"tbl_salary_range_map",engine)


# COMMAND ----------


