# Databricks notebook source
# essential imports
import sys
import os
import glob
from datetime import datetime
import pandas as pd
import numpy as np
import re
import time
import warnings

# commonfunc
import pkg_commonfunctions as cf
# connect to db
import pkg_dbconnect as db

# COMMAND ----------

# connect to database via modular script
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-nonsf-data")

# COMMAND ----------

# define all helper functions

#
def set_dtypes(df):
    df['persno'] = df['persno'].astype('string')
    df['personnel_number'] = df['personnel_number'].astype('string')
    df['orgunit'] = df['orgunit'].astype('string')
    df['company_code'] = df['company_code'].astype('string')
    df['organizational_unit'] = df['organizational_unit'].astype('string')
    df['cost_center'] = df['cost_center'].astype('string')
    df['job'] = df['job'].astype('string')
    df['gender_key'] = df['gender_key'].astype('category')
    df['nationality'] = df['nationality'].astype('string')
    df['fund_type'] = df['fund_type'].astype('category')
    df['cost_ctr'] = df['cost_ctr'].astype('string')
    df['personnel_subarea'] = df['personnel_subarea'].astype('string')
    
    df = df.select_dtypes(include=["string"]).apply(lambda x: x.str.strip())
    
    return df

def group_cleaning(df):
    df['orgunit'] = df.apply(lambda row: convert_toInt_toStr(row, 'orgunit'), axis=1)
    df['personnel_subarea'] = df.apply(lambda row: convert_toInt_toStr(row, 'personnel_subarea'), axis=1)
    df['personnel_subarea'] = df['personnel_subarea'].str.strip()
    df['personnel_subarea'] = df['personnel_subarea'].str.upper()
    df['persno'] = df.apply(lambda row: convert_toInt_toStr(row, 'persno'), axis=1)
    
    df['personnel_subarea'] = df.apply(lambda row: check_pers_subarea(row), axis=1)
    return df

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

# extra space = N/A
additionalNAValues = ["", " "]

def get_eom(row):
    month_ = row['_mth_no']
    year_ = row['_cyear']
    newdate = f'1/{month_}/{year_}'
    date_ = datetime.strptime(newdate,"%d/%m/%Y").date()  + pd.offsets.MonthEnd(0)
    return date_

# COMMAND ----------

def readLeaversExcel():
    filenames = '[2][0]*.xlsx'
    
    # iterate through files in selected folder
    dflist = []
    for filename in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(filename))
        print(f"{filename}, {fileCreationDate}")
        
        excel = pd.ExcelFile(filename)
        regex_string = 'Departure\Z'
        regex = re.compile(regex_string,re.I)
        sheets = [n for n in excel.sheet_names if regex.match(n)] # finds sheets based on string regex
        
        # loops through sheets and get df of each matching sheet
        dfs = []
        for s in sheets:
            datafile = pd.read_excel(excel,sheet_name=s,header=0, na_values=additionalNAValues)
            
            datafile = cf.strip_clean_drop(datafile)
            # add an extra column to record file name
            datafile['filename']=os.path.basename(filename)
            # add column for year/mth based on filename
            datename, ext = os.path.basename(filename).split(".")
            datafile['_cyear'] = datename[0:4]
            datafile['_mth_no'] = datename[5:7]
            datafile['_cyear'] = datafile['_cyear'].astype(int)
            datafile['_mth_no']= datafile['_mth_no'].astype(int)
            
            dfs.append(datafile)
        
        #combine dfs
        df = pd.concat(dfs,ignore_index=True)
        dflist.append(df)
    
    # overall combine dfs
    df = pd.concat(dflist,ignore_index=True)
    set_dtypes(df)
    
    #convert dates to dates accordingly
    df['date_joined'] = pd.to_datetime(df['date_joined'],dayfirst=True,errors='coerce')
    df['birth_date'] = pd.to_datetime(df['birth_date'],dayfirst=True,errors='coerce')
    df['date_left'] = pd.to_datetime(df['date_left'],dayfirst=True,errors='coerce')
    
    # ensure that date left is not null, if null use file's date
    df['date_left'] = df.apply(lambda row: check_dateleft(row, 'date_left'), axis=1)
    
    df = df.dropna(subset=['persno'])
    df = df.drop(df.filter(regex='unnamed').columns, axis=1)
    df = df.drop_duplicates(subset=['persno', 'personnel_number','date_left'], keep='first')

    df['_fyear'] = df.apply(lambda row: cat_fyear(row,'_mth_no','_cyear'), axis=1)
    
    df['_dateleft_month'] = df['date_left'].dt.month.astype('Int64', errors='ignore')
    df['_dateleft_year'] = df['date_left'].dt.year.astype('str', errors='ignore')
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    
    df['_dateleft_fy'] = df.apply(lambda row: cat_fyear(row,'_dateleft_month','_dateleft_year'), axis=1)
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)
    
    df['orgunit'] = df['orgunit'].fillna("")
    df['personnel_subarea'] = df['personnel_subarea'].fillna("N/A")
    df['persno'] = df['persno'].fillna(df['personnel_number'])
    
    df['orgunit'] = df.apply(lambda row: convert_toInt_toStr(row, 'orgunit'), axis=1)
    df['personnel_subarea'] = df.apply(lambda row: convert_toInt_toStr(row, 'personnel_subarea'), axis=1)
    df['personnel_subarea'] = df['personnel_subarea'].str.upper().str.strip()
    df['persno'] = df.apply(lambda row: convert_toInt_toStr(row, 'persno'), axis=1)
    
    df['active'] = False
    df['source'] = "Non-SF ETL"
    
    return df

# COMMAND ----------

def readHeadcountExcel():
    filenames = '[2][0]*.xlsx'
    
    # iterate through files in selected folder
    dflist = []
    for filename in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(filename))
        print(f"{filename}, {fileCreationDate}")

        excel = pd.ExcelFile(filename)
        regex_string = '\AName'
        regex = re.compile(regex_string,re.I)
        sheets = [n for n in excel.sheet_names if regex.match(n)] # finds sheets based on string regex
        
        # loops through sheets and get df of each matching sheet
        dfs = []
        for s in sheets:
            datafile = pd.read_excel(excel,sheet_name=s,header=0,na_values=additionalNAValues)
            datafile = cf.strip_clean_drop(datafile)
            # add an extra column to record file name
            datafile['filename']=os.path.basename(filename)
            datafile['filename']=os.path.basename(filename)
            # add column for year/mth based on filename
            datename, ext = os.path.basename(filename).split(".")
            datafile['_cyear'] = datename[0:4]
            datafile['_mth_no'] = datename[5:7]
            datafile['_cyear'] = datafile['_cyear'].astype(int)
            datafile['_mth_no']= datafile['_mth_no'].astype(int)
            
            datafile['_fyear'] = datafile.apply(lambda row: cat_fyear(row,'_mth_no','_cyear'), axis=1)
            datafile = datafile.drop_duplicates(subset=['persno', 'personnel_number'], keep='last')
            
            dfs.append(datafile)
        
        #combine dfs
        df = pd.concat(dfs,ignore_index=True)
        dflist.append(df)
    
    # overall combine dfs
    df = pd.concat(dflist,ignore_index=True)
    set_dtypes(df)
    
    #convert dates to dates accordingly
    df['date_joined'] = pd.to_datetime(df['date_joined'],dayfirst=True,errors='coerce')
    df['birth_date'] = pd.to_datetime(df['birth_date'],dayfirst=True,errors='coerce')
    
    df = df.dropna(how='all')
    df = df.drop(df.filter(regex='unnamed').columns, axis=1)
    
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)
    
    df['orgunit'] = df['orgunit'].fillna("")
    df['personnel_subarea'] = df['personnel_subarea'].fillna("N/A")
    df['persno'] = df['persno'].fillna(df['personnel_number'])
    
    df['orgunit'] = df.apply(lambda row: convert_toInt_toStr(row, 'orgunit'), axis=1)
    df['personnel_subarea'] = df.apply(lambda row: convert_toInt_toStr(row, 'personnel_subarea'), axis=1)
    df['personnel_subarea'] = df['personnel_subarea'].str.upper().str.strip()
    df['persno'] = df.apply(lambda row: convert_toInt_toStr(row, 'persno'), axis=1)
    
    df['active'] = True
    df['source'] = "Non-SF ETL"
    
    return df

# COMMAND ----------

def readPromotionExcel():
    filenames = '[2][0]*.xlsx'
    
    # iterate through files in selected folder
    dflist = []
    for filename in glob.glob(f'{root_dir}/{filenames}'):
        fileCreationDate = time.ctime(os.path.getctime(filename))
        print(f"{filename}, {fileCreationDate}")
        
        excel = pd.ExcelFile(filename)
        regex_string = '\AProm'
        regex = re.compile(regex_string,re.I)
        sheets = [n for n in excel.sheet_names if regex.match(n)] # finds sheets based on string regex
        
        # loops through sheets and get df of each matching sheet
        dfs = []
        for s in sheets:
            print(s)
            datafile = pd.read_excel(excel,sheet_name=s,header=0,na_values=additionalNAValues)
            datafile = cf.strip_clean_drop(datafile)
            # add an extra column to record file name
            datafile['filename']=os.path.basename(filename)

            dfs.append(datafile)
        
        #combine dfs
        df = pd.concat(dfs,ignore_index=True)
        dflist.append(df)
    
    # overall combine dfs
    df = pd.concat(dflist,ignore_index=True)
    #convert dates to dates accordingly
    df['start_date'] = pd.to_datetime(df['start_date'],dayfirst=True,errors='coerce')
    df.dropna(how='all')

    df = df.drop(df.filter(regex='unnamed').columns, axis=1)
    df = df.drop(df.filter(regex='retirement').columns, axis=1)
    df = df.drop(df.filter(regex='hourly').columns, axis=1)
    df.drop_duplicates(subset=['persno', 'personnel_number', 'start_date'], keep='first', inplace=True)
    
    return df

# COMMAND ----------

if root_dir != "":
    leavers_df = readLeaversExcel()
    leavers_df.info()
    headcount_df = readHeadcountExcel()
    headcount_df.info()
    combined_df = pd.concat([headcount_df, leavers_df], ignore_index=True)
    combined_df.info()
    
    promotions_df = readPromotionExcel()
    promotions_df.info()
else:
    raise Exception("Root Dir is empty, mounting of Blob was unsuccessful.")

# COMMAND ----------

combined_df.info()

# COMMAND ----------

combined_df = combined_df.convert_dtypes()
combined_df['orgunit'] = combined_df['company_code']
combined_df['eom'] = combined_df.apply(lambda row: get_eom(row), axis=1)
combined_df.info()

# COMMAND ----------

combined_df.eom.value_counts()

# COMMAND ----------

db.insert_with_progress(combined_df,"tbl_nonsf_sql_monthly",engine)

# COMMAND ----------

db.insert_with_progress(promotions_df,"tbl_nonsf_sql_monthly_promotions",engine)
