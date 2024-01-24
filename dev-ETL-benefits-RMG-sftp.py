# Databricks notebook source
# essential imports
import os
import glob
import pandas as pd
import numpy as np
import time
import re
import janitor

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-benefits-rmg")

# COMMAND ----------

raw_df = pd.read_csv(f"{root_dir}/202401_SATS_Transactions-OneTimeDraft.txt", header=0, sep='|',low_memory=False)
raw_df.sample(10)

# COMMAND ----------

def take_draftdata_rmg():
    filename = f"{root_dir}/202401_SATS_Transactions-OneTimeDraft.txt"
    fileCreationDate = time.ctime(os.path.getctime(filename))
    print(f"{filename}, {fileCreationDate}")
    try:
        df = pd.read_csv(filename, header=0, sep='|',low_memory=False)
    except:
        print(f"Reading {filename} failed.")
        pass
    
    df = cf.strip_clean_drop(df)
    df['filename'] = filename

    # further cleanups
    df = (
        df
        .clean_names(remove_special=True)
        .remove_empty()
    )

    # define date columns
    date_cols = [
        'treatmentdate','invoicedate','receivedate','ftp_giro_date'
    ]
    # convert to date if column exists
    for col in date_cols:
        if col in df.columns:
            print(f'checking: {col}')
            df[col] = df[col].astype(str).str.strip()
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
        else:
            # here, if not date, try and ensure that the other values properly casted to string
            df[col] = df[col].applymap(lambda x: re.sub(r'[^\w\s]', '', str(x)) if isinstance(x, str) else x)
            df[col] = df[col].astype(str).str.strip().str.upper()
    
    # define & convert number columns:
    no_cols = [
        'consultationamount','medicationamount','labamount','xrayamount','surgeryamount','otheramount','gst','total_amount_inclusive_of_gst','copayamount','ineligibleamount','sats_total','paid_by_payroll_bill_giro','sats_medisave','sats_medishield'
    ]
    # convert to number if column exists
    for col in no_cols:
        if col in df.columns:
            print(f'checking: {col}')
            #df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].astype('float',errors='ignore')

    df = df.sort_values(by='rmg_tpa_claimsregistrationnumber')
    df.info()
    return df

rmg_df = take_draftdata_rmg()

# COMMAND ----------

rmg_df.sample(5)

# COMMAND ----------

def take_draftdata_fwmi():
    filename = f"{root_dir}/202401_FWMI_Transactions-OneTimeDraft.xlsx"
    fileCreationDate = time.ctime(os.path.getctime(filename))
    print(f"{filename}, {fileCreationDate}")
    try:
        df = pd.read_excel(filename, sheet_name=0, header=1) 
    except:
        print(f"Reading {filename} failed.")
        pass
    
    df = cf.strip_clean_drop(df)
    df['filename'] = filename
    df.info()
    return df
 
#fwmi_df = take_draftdata_fwmi()

# COMMAND ----------

# melt columns into rows

# list of columns to be converted
cols_c1 = ['consultationamount','medicationamount','labamount','xrayamount','surgeryamount','otheramount','gst'] #'total_amount_inclusive_of_gst'
cols_c2 = ['copayamount','ineligibleamount','sats_total']
cols_c3 = ['paid_by_payroll_bill_giro','sats_medisave','sats_medishield']

# columns that will not be melted
id_vars = [
    'companycode','rmgcontractcode','rmg_company_name','rmg_tpa_claimsregistrationnumber','employeenumber','employeename','servicetype','visittypecode','treatmentdate','clinicname','diagnosis_icd10','referralby','systemsource','lognumber','logdate','invoicenumber','invoicedate','receivedate','ftp_giro_date','claimtype','claimstatus','costcenter','ca_nca','e_claim_manual_auto','rhi_ins_fwmi_claim_number','filename','total_amount_inclusive_of_gst'
]

# melt the dataframe multiple times and assign new column names
df1 = rmg_df.melt(id_vars=id_vars, value_vars=cols_c1, var_name='original_col', value_name='amount').assign(group='a_invoice_fees')
df2 = rmg_df.melt(id_vars=id_vars, value_vars=cols_c2, var_name='original_col', value_name='amount').assign(group='b_copay_splits')
df3 = rmg_df.melt(id_vars=id_vars, value_vars=cols_c3, var_name='original_col', value_name='amount').assign(group='c_payment_methods')
#df4 = rmg_df.melt(id_vars=id_vars, value_vars=cols_c4, var_name='var', value_name='value').assign(measure='c4')

# concatenate melted dataframes
rmg_df_long = pd.concat([df1, df2, df3], ignore_index=True)
rmg_df_long['amount'] = pd.to_numeric(rmg_df_long['amount'], errors='coerce')
rmg_df_long['amount'] = rmg_df_long['amount'].fillna(0)

# COMMAND ----------

rmg_df_long.info()

# COMMAND ----------

rmg_df_long['eom'] = pd.to_datetime(rmg_df_long['ftp_giro_date']) + pd.offsets.MonthEnd(0)

# COMMAND ----------

db.insert_with_progress(rmg_df_long,"tbl_benefits_rmg_sftp",engine)

# COMMAND ----------


