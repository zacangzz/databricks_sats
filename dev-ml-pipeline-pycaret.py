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

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-ml-pipelines")

# COMMAND ----------

# define all helper functions

# essential scripts for standalone scripting
# get age based on date
def calculate_age(df):
    today = df['todays_date'].where(df['date_left'].isna(), df['date_left'])
    birth_date = df['birth_date']

    df['age'] = ((today - birth_date).dt.days / 365.25).fillna(0).astype(int)
    df.loc[birth_date.isna(), 'age'] = 0

    return df

# get tenure/length of service
def calculate_tenure(df):
    today = df['todays_date'].where(df['date_left'].isna(), df['date_left'])
    date_joined = df['date_joined']

    df['tenure'] = (today - date_joined).dt.days / 365.25
    df['tenure'] = df['tenure'].round(2)
    df.loc[date_joined.isna(), 'tenure'] = pd.NA

    return df


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
    df['zip_code'] = df['zip_code'].astype('str',errors='ignore')
    df['work_schedule'] = df['work_schedule'].astype('category',errors='ignore')

    try:
        df["date_left"] = pd.to_datetime(df["date_left"], dayfirst=True, errors="coerce") # only applicable for leavers
    except:
        pass

    return df

def set_dtypes_2(df):
    df["position_entry_date"] = pd.to_datetime(df["position_entry_date"], dayfirst=True, errors="coerce")
    df["confirmation_date"] = pd.to_datetime(df["confirmation_date"], dayfirst=True, errors="coerce")
    df["probation_end_date"] = pd.to_datetime(df["probation_end_date"], dayfirst=True, errors="coerce")
    df["probation_extension_end_date"] = pd.to_datetime(df["probation_extension_end_date"], dayfirst=True, errors="coerce")

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
    
    df['fund_type'] = df['fund_type'].fillna("NA")
    # df['on_shift'] = np.where(df['work_schedule'] != 'Office Work Schedule', True, False)
    df['on_shift'] = np.where(df['work_schedule'].notna() & (df['work_schedule'] != 'Office Work Schedule'), True, False)

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

def cat_fyear(row, mthcol, yearcol):
    try:
        if row[mthcol] < 4:
            right_n = int(str(row[yearcol])[2:4])
            left_n = right_n - 1
            fyear = str(left_n) + str(right_n)
        else:
            left_n = int(str(row[yearcol])[2:4])
            right_n = left_n + 1
            fyear = str(left_n) + str(right_n)
        return fyear
    except:
        pass


def cat_cyear(row):
    if row["_mth_no"] < 4:
        cyear = "20" + row["_fyear"][2:4]
    else:
        cyear = "20" + row["_fyear"][0:2]
    return cyear

def check_dateleft(row, col):
    if pd.isna(row[col]):
        month_ = row['_mth_no']
        year_ = row['_cyear']
        newdate = f'1/{month_}/{year_}'
        date_ = datetime.strptime(newdate,"%d/%m/%Y").date()
        return date_
    else:
        return row[col]


# converts everything to float first, then convert back to int, to get rid of any decimals
def convert_toInt_toStr(row, col):
    # print(f'Starting value is {row[col]}, with Type: {type(row[col])}')
    try:
        # print("Trying...")
        a = str(round(float(row[col])))
        return a
    except:
        return str(row[col])



# COMMAND ----------

 def readLeaversCSV():
    dflist = []
    filenames = "*leavers*.csv"
    
    #
    files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    
    try:
        print(f"{latest_file}")
        df = pd.read_csv(latest_file, header=1)
    except:
        #print(f"Reading {latest_file} failed.")
        pass
    
    df = cf.strip_clean_drop(df)
    # remove record if date left is blank?
    df = df.dropna(subset=['date_left'])
    # remove duplicates if they are the same person with same date left
    df = df.drop_duplicates(subset=['persno', 'personnel_number','date_left'], keep='first')
    
    df = group_cleaning(df)
    df = set_dtypes(df)
    df = set_dtypes_2(df)
    
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    
    df['_dateleft_month'] = df['date_left'].dt.month.astype('Int64', errors='ignore')
    df['_dateleft_year'] = df['date_left'].dt.year.astype('str', errors='ignore')
    
    df['_dateleft_fy'] = df.apply(lambda row: cat_fyear(row,'_dateleft_month','_dateleft_year'), axis=1)
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)
    
    df['active'] = False
    
    return df

# COMMAND ----------

def readData():
    dflist = []
    filenames = "*.csv" # get the latest data
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"{file}, {fileCreationDate}")

        try:
            datafile = pd.read_csv(file, header=1, low_memory=False)
        except:
            print(f"Reading {file} failed.")
            pass
        datafile = cf.strip_clean_drop(datafile)
        dflist.append(datafile)
        # remove duplicates if they are the same person with same department (within the same file only)
        datafile = datafile.drop_duplicates(subset=['persno', 'personnel_number','orgunit'], keep='first', inplace=True)
        
    df = pd.concat(dflist, ignore_index=True)
    
    df = group_cleaning(df)
    df = set_dtypes(df)
    # df = set_dtypes_2(df)

    # Identify 'date_joined' corresponding to the latest 'todays_date' for each 'persno'
    latest_date_join = df.sort_values('todays_date').groupby('persno').last()['date_joined']
    # Map these latest 'date_joined' values back to the original dataframe
    df['date_joined'] = df['persno'].map(latest_date_join)
    
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)

    df['_dateleft_month'] = df['date_left'].dt.month.astype('Int64', errors='ignore')
    df['_dateleft_year'] = df['date_left'].dt.year.astype('str', errors='ignore')
    df['_dateleft_fy'] = df.apply(lambda row: cat_fyear(row,'_dateleft_month','_dateleft_year'), axis=1)

    return df

# COMMAND ----------

if root_dir != "":
    dataset = readData()
    dataset['personnel_subarea'] = dataset.apply(lambda row: check_pers_subarea(row),axis=1)
    dataset['source'] = "SF Direct"
    
else:
    raise Exception("Root Dir is empty, mounting of Blob was unsuccessful.")

# COMMAND ----------

# global transformations
dataset = calculate_age(dataset)
dataset = calculate_tenure(dataset)

# COMMAND ----------

# get latest ref mapping tables

ref_grade = pd.read_sql('SELECT * FROM tbl_reftable_grade', engine)
ref_bu = pd.read_sql('SELECT * FROM tbl_reftable_bu', engine)
ref_company = pd.read_sql('SELECT * FROM tbl_reftable_company', engine)
ref_attrition = pd.read_sql('SELECT * FROM tbl_reftable_attrition', engine)
ref_jobgroup = pd.read_sql('SELECT * FROM tbl_reftable_jobgroup', engine)

ref_grade.info()
ref_bu.info()
ref_company.info()
ref_attrition.info()
ref_jobgroup.info()

# COMMAND ----------

dataset['personnel_subarea'] = dataset['personnel_subarea'].str.strip().str.upper()
dataset['company_code'] = dataset['company_code'].str.strip().str.upper()
dataset['orgunit'] = dataset['orgunit'].str.strip().str.upper()
#dataset['reason_for_action'] = dataset['reason_for_action'].str.strip().str.upper()
dataset['job'] = dataset['job'].str.strip().str.upper()

# COMMAND ----------

# complete vlookup by merging
dataset = pd.merge(dataset, ref_grade, on="personnel_subarea",how='left')
dataset = pd.merge(dataset, ref_bu, on='orgunit',how='left', suffixes=(None, '_ref'))
dataset = pd.merge(dataset, ref_company, on='company_code',how='left')
#dataset = pd.merge(dataset, ref_attrition, left_on='reason_for_action', right_on='attrition_reason',how='left')
dataset = pd.merge(dataset, ref_jobgroup, on='job',how='left')

# COMMAND ----------

# fill NAs
dataset['gender_key'] = dataset['gender_key'].fillna('M')

# COMMAND ----------

dataset['active'] = dataset['date_left'].apply(lambda x: False if pd.isnull(x) else True)

# COMMAND ----------

dataset.info()

# COMMAND ----------

dataset.to_parquet('/tmp/dataset.parquet')

# COMMAND ----------

dbutils.fs.cp('file:/tmp/dataset.parquet', 'dbfs:/user/hive/warehouse/dataset.parquet')

# COMMAND ----------

# Read the Parquet file into a DataFrame
df = spark.read.parquet("dbfs:/user/hive/warehouse/dataset.parquet")

# Write the DataFrame out as a Hive table
df.write.mode("overwrite").saveAsTable("dataset")

# COMMAND ----------

df.select("active").distinct().count()

# COMMAND ----------

db.insert_with_progress(dataset,"ml_pipeline_train",engine)

# COMMAND ----------

from pycaret.classification import *
s = setup(dataset, target='active')
