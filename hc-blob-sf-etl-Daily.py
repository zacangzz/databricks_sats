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
root_dir = db.connect_AzureBlob("hc-sf-data")

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
    df['zip_code'] = df['zip_code'].astype('str',errors='ignore')
    df['work_schedule'] = df['work_schedule'].astype('category',errors='ignore')
    #df["position_start_date"] = pd.to_datetime(df["position_start_date"], dayfirst=True, errors="coerce")
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


def str_todate(row):
    date_time_str = "18/09/19 01:55:19"
    date_time_obj = datetime.strptime(date_time_str, "%d/%m/%y %H:%M:%S")

    return date_time_obj

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

def calculate_age(df, birth_date_col):
    today = pd.Timestamp(date.today())
    age = (today - df[birth_date_col]).astype('timedelta64[D]') / 365.25
    age[df[birth_date_col].isna()] = 0
    return round(age, 2)


# COMMAND ----------

 def readLeaversCSV():
    dflist = []
    filenames = "*leavers*.csv"
    
    #
    files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    
    try:
        print(f"{files}")
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
    
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    
    df['_dateleft_month'] = df['date_left'].dt.month.astype('Int64', errors='ignore')
    df['_dateleft_year'] = df['date_left'].dt.year.astype('str', errors='ignore')
    
    df['_dateleft_fy'] = df.apply(lambda row: cat_fyear(row,'_dateleft_month','_dateleft_year'), axis=1)
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)
    
    df['active'] = False
    
    return df

# COMMAND ----------

def readHeadcountCSV():
    dflist = []
    filenames = "*headcount*.csv"
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

    df["position_entry_date"] = pd.to_datetime(df["position_entry_date"], dayfirst=True, errors="coerce")
    
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)
    df['active'] = True
    
    return df

# COMMAND ----------

def readFlexiTempCSV():
    dflist = []
    filenames = "*flexi*.csv"
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

    df["position_entry_date"] = pd.to_datetime(df["position_entry_date"], dayfirst=True, errors="coerce")
    
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)
    df['active'] = True
    
    return df


# COMMAND ----------

def readPromotionCSV():
    dflist = []
    filenames = "*promotion*.csv"
    
    files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    
    try:
        print(f"{latest_file}")
        df = pd.read_csv(latest_file, header=1)
    except:
        print(f"Reading {latest_file} failed.")
        pass
    df = cf.strip_clean_drop(df)
    df["start_date"] = pd.to_datetime(df["start_date"], dayfirst=True, errors="coerce")
    df["todays_date"] = pd.to_datetime(df["todays_date"], dayfirst=True, errors="coerce")
    df['persno'] = df.apply(lambda row: convert_toInt_toStr(row, 'persno'), axis=1)
    df = df.drop_duplicates(subset=['persno', 'personnel_number','start_date'], keep='first')
    '''
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"{file}, {fileCreationDate}")

        try:
            datafile = pd.read_csv(file, header=1) 
        except:
            print(f"Reading {file} failed.")
            pass
        strip_clean_drop(datafile)
        dflist.append(datafile)

    df = pd.concat(dflist, ignore_index=True)
    df["effective start date"] = pd.to_datetime(df["effective start date"], dayfirst=True, errors="coerce")
    df["today's date"] = pd.to_datetime(df["today's date"], dayfirst=True, errors="coerce")
    df['persno'] = df.apply(lambda row: convert_toInt_toStr(row, 'persno'), axis=1)
    '''
    
    return df

# COMMAND ----------

df_onemap = pd.read_sql(
    '''
    SELECT * 
    FROM ref_onemapsg
    '''
    ,engine
)
df_onemap.info()

# COMMAND ----------

if root_dir != "":
    leavers_df = readLeaversCSV()
    headcount_df = readHeadcountCSV()
    df_etl_sf = pd.concat([headcount_df, leavers_df], ignore_index=True)
    df_etl_sf['personnel_subarea'] = df_etl_sf.apply(lambda row: check_pers_subarea(row),axis=1)
    df_etl_sf['source'] = "SF Direct"
    df_etl_sf['eom'] = pd.to_datetime(df_etl_sf['todays_date']) + pd.offsets.MonthEnd(0)
    df_etl_sf = pd.merge(df_etl_sf, df_onemap, on='zip_code', how='left')

    flexitemp_df = readFlexiTempCSV()
    flexitemp_df['personnel_subarea'] = flexitemp_df.apply(lambda row: check_pers_subarea(row),axis=1)
    flexitemp_df['source'] = "SF Direct"
    flexitemp_df['eom'] = pd.to_datetime(flexitemp_df['todays_date']) + pd.offsets.MonthEnd(0)
    flexitemp_df = pd.merge(flexitemp_df, df_onemap, on='zip_code', how='left')

    promotions_df = readPromotionCSV()
    
else:
    raise Exception("Root Dir is empty, mounting of Blob was unsuccessful.")

# COMMAND ----------

df_etl_sf.query(" date_left.notnull() ").groupby('todays_date').count()
#df_etl_sf.groupby('todays_date').count()

# COMMAND ----------

df_etl_sf.info()

# COMMAND ----------

df_etl_sf.groupby(['eom','active']).persno.nunique()

# COMMAND ----------

df_etl_sf.eom.value_counts()

# COMMAND ----------

df_etl_sf.todays_date.value_counts().sort_index()

# COMMAND ----------

db.insert_with_progress(df_etl_sf,"tbl_sf_sql_daily",engine)

# COMMAND ----------

db.insert_with_progress(promotions_df,"tbl_sf_sql_daily_promotions",engine)

# COMMAND ----------

db.insert_with_progress(flexitemp_df,"tbl_sf_sql_daily_flexi_temp",engine)

# COMMAND ----------

flexitemp_df.info()

# COMMAND ----------

df_etl_sf.info()

# COMMAND ----------


