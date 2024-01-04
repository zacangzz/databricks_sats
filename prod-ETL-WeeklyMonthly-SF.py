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
# import holidays
# from dateutil.relativedelta import relativedelta

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-sf-data")

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

    df = cf.strip_clean_drop(df)

    df = df.dropna(subset='relationship')
    df['relationship'] = df['relationship'].astype('category')
    df["dependent_date_of_birth"] = pd.to_datetime(df["dependent_date_of_birth"], dayfirst=True, errors="coerce")
    df = df.drop_duplicates(subset=['persno','relationship','dependent_first_name','dependent_date_of_birth'], keep='first')

    df['todays_date'] = date.today()
    df = cf.calculate_age(df, 'dependent_date_of_birth', 'd_age',today=True)
    #df['d_age'] = calculate_age(df, 'dependent_date_of_birth')
    
    df.info()
    return df
dependant_df = readDependant()

# COMMAND ----------

dependant_df.head()

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

    df = cf.strip_clean_drop(df)
    print(df.columns)

    df["date_attained"] = pd.to_datetime(df["date_attained"], dayfirst=True, errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], dayfirst=True, errors="coerce")
    
    df = df.dropna(subset='qualification').sort_values(by='persno')
    df['is_highest_qual'] = df['is_highest_qual'].astype('category')

    df['latest_qual'] = df.groupby('persno')['date_attained'].transform('max') == df['date_attained']

    return df

edu_df = readEducation()

# COMMAND ----------

def readTimeOff():

    dflist = []
    filenames = "*[tT]ime[oO]ff*.csv"
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"{file}, {fileCreationDate}")

        try:
            datafile = pd.read_csv(file, header=1, low_memory=False)
        except:
            print(f"Reading {file} failed.")
            pass
        datafile = cf.strip_clean_drop(datafile)
        datafile["persno"] = datafile['persno'].astype('string')
        datafile["startdate"] = pd.to_datetime(datafile["startdate"], dayfirst=True, errors="coerce")
        datafile["enddate"] = pd.to_datetime(datafile["enddate"], dayfirst=True, errors="coerce")
        datafile["todays_date"] = pd.to_datetime(datafile["todays_date"], dayfirst=True, errors="coerce")
        datafile = datafile.drop_duplicates(subset=['persno','leave_type','startdate','enddate'], keep='first')
        datafile = datafile.sort_values(by=['persno','startdate'],ascending=True)

        dflist.append(datafile)
    
    df = pd.concat(dflist, ignore_index=True)
    df['orgunit'] = df['orgunit'].astype('string').fillna("")
    df.info()
    
    
    '''files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    print(f"{latest_file}")

    try:
        df = pd.read_csv(latest_file, header=1)
        print(df.shape)
    except:
        print(f"Reading {latest_file} failed.")
        pass

    df = strip_clean_drop(df)
    df["startdate"] = pd.to_datetime(df["startdate"], dayfirst=True, errors="coerce")
    #df["leave_start_date"] = pd.to_datetime(df["leave_start_date"], dayfirst=True, errors="coerce")
    #df["leave_end_date"] = pd.to_datetime(df["leave_end_date"], dayfirst=True, errors="coerce")

    df = df.drop_duplicates(subset=['persno','leave_type','startdate','enddate'], keep='first')
    
    #remove na dates across the board:
    df = df.dropna()'''
    
    '''# count is negative now, convert to positive:
    df['count'] = df['count'].apply(lambda x: x*-1)'''
    
    '''# get day info, mark weekends
    df["weekday_no"] = df['leave_date'].dt.weekday
    df["weekday"] = df['leave_date'].dt.day_name()
    df['is_weekend'] = df['weekday_no'].apply(lambda x: True if (x > 4) else False)'''
    
    '''# get list of holidays in Singapore, mark holidays
    holidays_sg = holidays.Singapore(years=[2022, 2023])
    df['is_holiday'] = df['leave_date'].apply(lambda x: True if (x in holidays_sg) else False)'''
    
    return df

timeoff_df = readTimeOff()
#timeoff_df.head(10)
#timeoff_df.query('leave_date != date_exploded')

# COMMAND ----------

def explodeTimeOff(df):
    # take record level data and explode into individual dates
    # explode dates to individual rows
    df = df[['persno','leave_type_code','leave_type','startdate','enddate','duration_days','orgunit']].copy()
    df['date_exploded'] = [pd.date_range(s, e, freq='d') for s, e in
              zip(pd.to_datetime(df['startdate']),
                  pd.to_datetime(df['enddate']))]
    df = df.explode('date_exploded',ignore_index=True)
    df = df.sort_values(by='date_exploded',ascending=True)

    df.info()
    
    return df

timeoff_df_exploded = explodeTimeOff(timeoff_df)


# COMMAND ----------



# COMMAND ----------


'''# explode dates to individual rows
timeoff_df['date_exploded'] = [pd.date_range(s, e, freq='d') for s, e in
            zip(pd.to_datetime(timeoff_df['startdate']),
                pd.to_datetime(timeoff_df['enddate']))]
timeoff_df = timeoff_df.explode('date_exploded',ignore_index=True)
timeoff_df = timeoff_df.sort_values(by=['persno', 'date_exploded'])

# find gaps between each leave records
timeoff_df['gap'] = timeoff_df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff() > pd.to_timedelta('1 day')
#timeoff_df['gap_days'] = timeoff_df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff()
timeoff_df['gap_days'] = timeoff_df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff()['date_exploded'].dt.days.fillna(0)
timeoff_df['gap_days_1'] = timeoff_df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff()

# find consecutive dates between each record
timeoff_df['consecutive'] = timeoff_df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff() <= pd.to_timedelta('1 day')
timeoff_df['consecutive_days'] = timeoff_df['consecutive'].astype(int).groupby((timeoff_df['consecutive'] != timeoff_df['consecutive'].shift()).cumsum()).cumsum()'''

# COMMAND ----------

timeoff_df.head(20)

# COMMAND ----------

def process_timeoff(df):
    df = df.copy()
    # Explode dates to individual rows
    df['date_exploded'] = [pd.date_range(s, e, freq='d') for s, e in
                zip(pd.to_datetime(df['startdate']),
                    pd.to_datetime(df['enddate']))]
    df = df.explode('date_exploded',ignore_index=True)
    df = df.sort_values(by=['persno', 'date_exploded'])

    # Find gaps between each leave records
    df['gap'] = df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff() > pd.to_timedelta('1 day')
    #df['gap_days'] = df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff()
    df['gap_days'] = (df[['persno', 'date_exploded']]
                    .sort_values(by=['persno', 'date_exploded'])
                    .groupby('persno')
                    .diff()['date_exploded']
                    .dt.days
                    .fillna(np.nan))

    # Find consecutive dates between each record
    df['consecutive'] = df[['persno', 'date_exploded']].sort_values(by=['persno', 'date_exploded']).groupby('persno').diff() <= pd.to_timedelta('1 day')
    df['consecutive_days'] = df['consecutive'].astype(int).groupby((df['consecutive'] != df['consecutive'].shift()).cumsum()).cumsum()

    #drop the duplicated exploded data
    df = df.sort_values(by=['persno', 'leave_type_code','startdate','enddate','duration_days','date_exploded'],ascending=True)
    df = df.drop_duplicates(subset=['persno', 'leave_type_code','startdate','enddate','duration_days'], keep='last')
    return df

timeoff_gap_consec_df = process_timeoff(timeoff_df)

# COMMAND ----------

timeoff_gap_consec_df.info()

# COMMAND ----------

timeoff_gap_consec_df.head()

# COMMAND ----------

def readWorkPass():
    filenames = "*workpermit*.csv"
    
    files = glob.glob(f"{root_dir}/{filenames}")
    latest_file = max(files, key=os.path.getctime)
    print(f"{latest_file}")

    try:
        df = pd.read_csv(latest_file, header=1)
    except:
        print(f"Reading {latest_file} failed.")
        pass

    df = cf.strip_clean_drop(df)

    df["expiry_date"] = pd.to_datetime(df["expiry_date"], dayfirst=True, errors="coerce")
    df["issue_date"] = pd.to_datetime(df["issue_date"], dayfirst=True, errors="coerce")
    df["todays_date"] = pd.to_datetime(df["todays_date"], dayfirst=True, errors="coerce")
    df['doc_type'] = df['doc_type'].astype('category')
    df['country'] = df['country'].astype('category')
    df['persno'] = df['persno'].astype('string')
    df['mom_indicator'] = df['mom_indicator'].astype('category')

    df = df.sort_values(by=['persno','issue_date'],ascending=False)
    df = df.drop_duplicates(subset=['persno'],keep='first')
    
    df.info()

    return df

wp_df = readWorkPass()

# COMMAND ----------

db.insert_with_progress(wp_df,"tbl_sf_sql_workpasses",engine)


# COMMAND ----------

db.insert_with_progress(dependant_df,"tbl_sf_sql_dependant",engine)



# COMMAND ----------

db.insert_with_progress(edu_df,"tbl_sf_sql_edu",engine)

# COMMAND ----------

db.insert_with_progress(timeoff_df,"tbl_sf_sql_timeoff",engine)

# COMMAND ----------

db.insert_with_progress(timeoff_gap_consec_df,"tbl_sf_sql_timeoff_gaps",engine)

# COMMAND ----------

db.insert_with_progress(timeoff_df_exploded,"tbl_sf_sql_timeoff_exploded",engine)

# COMMAND ----------


