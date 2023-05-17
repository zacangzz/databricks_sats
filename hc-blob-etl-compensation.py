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
import warnings
# from zipfile import ZipFile
import holidays
from dateutil.relativedelta import relativedelta

# commonfunc
import pkg_commonfunctions as cf
# connect to db
import pkg_dbconnect as db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-cb-info")

# COMMAND ----------

# define all helper functions
#

def set_dtypes(df):
    df["date_joined"] = pd.to_datetime(df["date_joined"], dayfirst=True, errors="coerce")
    df["birth_date"] = pd.to_datetime(df["birth_date"], dayfirst=True, errors="coerce")
    df["service_date"] = pd.to_datetime(df["service_date"], dayfirst=True, errors="coerce")
    df["todays_date"] = pd.to_datetime(df["todays_date"], dayfirst=True, errors="coerce")
    df['persno'] = df['persno'].astype('string')
    df['company_code'] = df['company_code'].astype('category')
    df['organizational_unit'] = df['organizational_unit'].astype('category')
    df['cost_center'] = df['cost_center'].astype('string')
    df['employee_group'] = df['employee_group'].astype('category')
    df['position'] = df['position'].astype('string')
    df['job'] = df['job'].astype('string')
    df['personnel_subarea'] = df['personnel_subarea'].astype('category')
    df['gender_key'] = df['gender_key'].astype('category')
    df['nationality'] = df['nationality'].astype('category',errors='ignore')
    df['employee_sub_group'] = df['employee_sub_group'].astype('category',errors='ignore')
    df['fund_type'] = df['fund_type'].astype('category',errors='ignore')
    df['marital_status'] = df['marital_status'].astype('category',errors='ignore')
    df['pay_scale_group'] = df['pay_scale_group'].astype('category',errors='ignore')
    df['zip_code'] = df['zip_code'].astype('int',errors='ignore')
    df['work_schedule'] = df['work_schedule'].astype('category',errors='ignore')
    try:
        df["date_left"] = pd.to_datetime(df["date_left"], dayfirst=True, errors="coerce") # only applicable for leavers
    except:
        pass

    return df

def group_cleaning(df):
    df['orgunit'] = df.apply(lambda row: convert_toInt_toStr(row, 'orgunit'), axis=1)
    df['personnel_subarea'] = df.apply(lambda row: convert_toInt_toStr(row, 'personnel_subarea'), axis=1)
    df['personnel_subarea'] = df['personnel_subarea'].str.strip()
    df['personnel_subarea'] = df['personnel_subarea'].str.upper()
    df['persno'] = df.apply(lambda row: convert_toInt_toStr(row, 'persno'), axis=1)
    df['persno'] = df['persno'].astype('string')
    df['zip_code'] = df.apply(lambda row: convert_toInt_toStr(row, 'zip_code'), axis=1)
    df['personnel_subarea'] = df.apply(lambda row: check_pers_subarea(row), axis=1)
    df['reporting_officer'] = df['reporting_officer'].replace("NO_MANAGER",np.nan)
    
    df['on_shift'] = np.where(df['work_schedule'] != 'Office Work Schedule', True, False)

    return df

# check for Managerial Grade & replace it with H1-9
def check_pers_subarea(row):
    if "MANAGERIAL GRADE" in row['personnel_subarea']:
        if pd.isna(row['pay_scale_group']) or ("Def for NA" in row['pay_scale_group']) or ("OTHERS" in row['pay_scale_group']):
            return row['personnel_subarea']
        else:
            return row['pay_scale_group']
    else:
        return row['personnel_subarea']
    
    return

# this is the dictionary to map months to numbers
month_no_dict = {
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
}

def str_todate(row):
    date_time_str = "18/09/19 01:55:19"
    date_time_obj = datetime.strptime(date_time_str, "%d/%m/%y %H:%M:%S")

    return date_time_obj

def calculate_age(df, birth_date_col):
    today = pd.Timestamp(date.today())
    age = (today - df[birth_date_col]).astype('timedelta64[D]') / 365.25
    age[df[birth_date_col].isna()] = 0
    return round(age, 2)


# COMMAND ----------

def readComp():
    dflist = []
    filenames = "*Compensation*.csv"
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"{file}, {fileCreationDate}")

        try:
            datafile = pd.read_csv(file, header=1, low_memory=False)
        except:
            print(f"Reading {file} failed.")
            pass
        datafile = cf.strip_clean_drop(datafile)
        datafile = datafile.dropna(subset=['total_monthly_basic'])
        datafile = datafile.drop_duplicates(subset=['persno', 'total_monthly_basic','event_picklist_label','effective_start_date'], keep='first')

        datafile = datafile[datafile.total_monthly_basic != 0]
        # remove duplicates if they are the same person with same department (within the same file only)
        #datafile = datafile.drop_duplicates(subset=['persno', 'personnel_number','orgunit'], keep='first', inplace=True)
        dflist.append(datafile)
    
    df = pd.concat(dflist, ignore_index=True)

    df["todays_date"] = pd.to_datetime(df["todays_date"], dayfirst=True, errors="coerce")
    df["effective_start_date"] = pd.to_datetime(df["effective_start_date"], dayfirst=True, errors="coerce")
    df['eom'] = pd.to_datetime(df['todays_date']) + pd.offsets.MonthEnd(0)

    print(df.info())
    return df



# COMMAND ----------

def readMercerData():
    filename = f"{root_dir}/MercerData.xlsx"
    
    try:
        df_mercerdata = pd.read_excel(filename, sheet_name=4, header=1)
    except:
        print(f"Reading {filename} failed.")
        pass
    df_mercerdata = cf.strip_clean_drop(df_mercerdata)

    return df_mercerdata
mercerdata_df = readMercerData()
mercerdata_df.info()

# COMMAND ----------

d_mask = ['job_title','job_type','job_code','position_class','base_salary_median_iw']

# COMMAND ----------

md_df = mercerdata_df[d_mask].copy()

# COMMAND ----------

md_df.job_title.value_counts()

# COMMAND ----------

md_df[['job_title_s1','job_title_s2']] = md_df.job_title.str.split(" - ", n=1, expand=True)


# COMMAND ----------

md_df.job_title_s1.sort_values().unique()[200:250]

# COMMAND ----------

if root_dir != "":
    comp_df = readComp()
    
else:
    raise Exception("Root Dir is empty, mounting of Blob was unsuccessful.")


# COMMAND ----------

db.insert_with_progress(comp_df,"tbl_sf_sql_comp",engine)
