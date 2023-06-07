# Databricks notebook source
# essential imports
import sys
import os
import glob
import pandas as pd
import numpy as np
import re
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
root_dir = db.connect_AzureBlob("hc-reftable")

# COMMAND ----------

# define all helper functions

def str_todate(row):
    date_time_str = "18/09/19 01:55:19"
    date_time_obj = datetime.strptime(date_time_str, "%d/%m/%y %H:%M:%S")

    return date_time_obj

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

def loadSFOrgStructure():
    filename = f"{root_dir}/sf-orgstructure-Component1.csv"

    try:
        df_sforg = pd.read_csv(filename, header=1,dtype={'Org.Unit':str})
    except:
        print(f"Reading {filename} failed.")
        pass

    df_sforg = cf.strip_clean_drop(df_sforg)
    df_sforg = df_sforg[['business_unit_label','orgunit','orgunit_name','parentdepartment_label']].copy()
    df_sforg.columns = {'business_unit','orgunit','organizational_unit','parent_department'}

    df_sforg['parent_department'] = df_sforg['parent_department'].fillna(value=df_sforg['business_unit'])
    df_sforg = df_sforg.apply(lambda x: x.str.upper() if x.dtypes=="string" else x)
    return df_sforg

sforg_df = loadSFOrgStructure()


# COMMAND ----------

sforg_df.info()

# COMMAND ----------

sforg_df.sample(10)

# COMMAND ----------

def loadreferencetables():
    filename = f"{root_dir}/HC_HeadCount_RefTables.xlsx"
    
    try:
        df_company = pd.read_excel(filename, sheet_name=0, header=0, na_filter=False)
        df_grade = pd.read_excel(filename, sheet_name=2, header=0, na_filter=False, dtype={'Personnel Subarea':str})
        df_bu = pd.read_excel(filename, sheet_name=1, header=0, na_filter=True, dtype={'Org.unit':str})
        
        df_nationality = pd.read_excel(filename, sheet_name=5, header=0, na_filter=True)
        df_attrition = pd.read_excel(filename, sheet_name=6, header=0, na_filter=True)
        df_agegroup = pd.read_excel(filename, sheet_name=7, header=0, na_filter=True)
        df_losgroup = pd.read_excel(filename, sheet_name=8, header=0, na_filter=True)
        df_jobgroup = pd.read_excel(filename, sheet_name=9, header=0, na_filter=True)
        
    except:
        print(f"Reading {filename} failed.")
        pass
    
    df_company = cf.strip_clean_drop(df_company)
    df_grade = cf.strip_clean_drop(df_grade)
    df_bu = cf.strip_clean_drop(df_bu)
    df_jobgroup = cf.strip_clean_drop(df_jobgroup)
    df_attrition = cf.strip_clean_drop(df_attrition)
    
    # for consistency
    df_bu = df_bu.apply(lambda x: x.str.upper() if x.dtypes=="string" else x)
    df_grade = df_grade.apply(lambda x: x.str.upper() if x.dtypes=="string" else x)

    # fix strings & integers
    df_company = df_company.rename(columns={"company_id": "company_code"})

    # ensure no blanks
    df_bu['parent_department'] = df_bu['parent_department'].fillna(value=df_bu['business_unit'])

    # clean up for vlookup
    df_grade['personnel_subarea'] = df_grade['personnel_subarea'].str.upper()
    df_company['company_code'] = df_company['company_code'].str.strip().str.upper()
    df_bu['orgunit'] = df_bu['orgunit'].str.strip().str.upper()
    df_attrition['attrition_reason'] = df_attrition['attrition_reason'].str.strip().str.upper()
    df_jobgroup['job'] = df_jobgroup['job'].str.strip().str.upper()

    return df_company, df_grade, df_bu, df_jobgroup, df_attrition

# COMMAND ----------

if root_dir != "":
    ref_company, ref_grade, ref_bu, ref_jobgroup, ref_attrition = loadreferencetables()
    
    ref_company.info()
    ref_grade.info()
    ref_bu.info()
    
    ref_jobgroup.info()
    ref_attrition.info()
else:
    raise Exception("Root Dir is empty, mounting of Blob was unsuccessful.")

# COMMAND ----------

sforg_df = sforg_df.set_index('orgunit')
sforg_df

# COMMAND ----------

ref_bu = ref_bu.set_index('orgunit')
ref_bu

# COMMAND ----------

#sforg_df = sforg_df.set_index('orgunit')
#ref_bu = ref_bu.set_index('orgunit')
ref_bu.update(sforg_df)
ref_bu = ref_bu.reset_index()

# COMMAND ----------

ref_bu

# COMMAND ----------

db.insert_with_progress(ref_company,"tbl_reftable_company",engine)

# COMMAND ----------

db.insert_with_progress(ref_grade,"tbl_reftable_grade",engine)

# COMMAND ----------

db.insert_with_progress(ref_bu,"tbl_reftable_bu",engine)

# COMMAND ----------

db.insert_with_progress(ref_jobgroup,"tbl_reftable_jobgroup",engine)

# COMMAND ----------

db.insert_with_progress(ref_attrition,"tbl_reftable_attrition",engine)

# COMMAND ----------


