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

def strip_clean_drop(dataframe):
    dataframe.columns = dataframe.columns.str.strip()  # gets rid of extra spaces
    dataframe.columns = dataframe.columns.str.lower()  # converts to lower case
    dataframe.columns = dataframe.columns.str.replace(' ', '_')  # allow dot notation with no spaces
    dataframe.columns = dataframe.columns.str.replace("'", '')  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace("(", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace(")", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace(".", '', regex=False)  # allow dot notation with no special chars
    dataframe = dataframe.dropna(axis=0, how="all")
    dataframe = dataframe.dropna(axis=1, how="all")
    dataframe = dataframe.convert_dtypes(convert_string=True)
    
    try:
        dataframe = dataframe.apply(lambda x: x.str.strip() if x.dtypes=="string" else x)
    except:
        pass
    
    return dataframe

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

def readDependant():
    filenames = "*Dependant*.csv"
    
    files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    print(f"{latest_file}")

    try:
        df = pd.read_csv(latest_file, header=1)
    except:
        print(f"Reading {latest_file} failed.")
        pass

    df = strip_clean_drop(df)

    df = df.dropna(subset='relationship')
    df['relationship'] = df['relationship'].astype('category')
    df["dependent_date_of_birth"] = pd.to_datetime(df["dependent_date_of_birth"], dayfirst=True, errors="coerce")
    df = df.drop_duplicates(subset=['persno','relationship','dependent_first_name','dependent_date_of_birth'], keep='first')

    df['todays_date'] = date.today()
    df['d_age'] = calculate_age(df, 'dependent_date_of_birth')
    
    df.info()

    return df

# COMMAND ----------

def readEducation():
    filenames = "*Education*.csv"
    
    files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    print(f"{latest_file}")

    try:
        df = pd.read_csv(latest_file, header=1)
        #df.info()
        print(df.shape)
    except:
        print(f"Reading {latest_file} failed.")
        pass

    df = strip_clean_drop(df)

    df = df.dropna(subset='is_highest_qual')
    df['is_highest_qual'] = df['is_highest_qual'].astype('category')


    return df

# COMMAND ----------

def readTimeOff():
    filenames = "*TimeOff*.csv"
    
    files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    print(f"{latest_file}")

    try:
        df = pd.read_csv(latest_file, header=1)
        #df.info()
        print(df.shape)
    except:
        print(f"Reading {latest_file} failed.")
        pass

    df = strip_clean_drop(df)
    df["leave_date"] = pd.to_datetime(df["leave_date"], dayfirst=True, errors="coerce")
    #df["leave_start_date"] = pd.to_datetime(df["leave_start_date"], dayfirst=True, errors="coerce")
    #df["leave_end_date"] = pd.to_datetime(df["leave_end_date"], dayfirst=True, errors="coerce")
    df.info()
    df = df.drop_duplicates(subset=['persno','leave_type','leave_date'], keep='first')
    print(df.shape)
    
    #remove na dates across the board:
    df = df.dropna()
    
    # count is negative now, convert to positive:
    df['count'] = df['count'].apply(lambda x: x*-1)
    
    # get day info, mark weekends
    df["weekday_no"] = df['leave_date'].dt.weekday
    df["weekday"] = df['leave_date'].dt.day_name()
    df['is_weekend'] = df['weekday_no'].apply(lambda x: True if (x > 4) else False)
    
    # get list of holidays in Singapore, mark holidays
    holidays_sg = holidays.Singapore(years=[2022, 2023])
    df['is_holiday'] = df['leave_date'].apply(lambda x: True if (x in holidays_sg) else False)
    
    #not correct
    '''# explode dates to individual rows
    df['date_exploded'] = [pd.date_range(s, e, freq='d') for s, e in
              zip(pd.to_datetime(df['leave_start_date']),
                  pd.to_datetime(df['leave_end_date']))]
    df = df.explode('date_exploded',ignore_index=True)'''
    
    
    df = df.sort_values(by='leave_date',ascending=True)
    
    print(df.shape)
    df.info()
    return df

#timeoff_df = readTimeOff()
#timeoff_df.head(10)
#timeoff_df.query('leave_date != date_exploded')

# COMMAND ----------

if root_dir != "":
    timeoff_df = readTimeOff()
    edu_df = readEducation()
    dependant_df = readDependant()
    
else:
    raise Exception("Root Dir is empty, mounting of Blob was unsuccessful.")

# COMMAND ----------

db.insert_with_progress(dependant_df,"tbl_sf_sql_dependant",engine)
db.insert_with_progress(edu_df,"tbl_sf_sql_edu",engine)
db.insert_with_progress(timeoff_df,"tbl_sf_sql_timeoff",engine)

# COMMAND ----------


