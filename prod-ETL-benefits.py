# Databricks notebook source
# essential imports
import os
import glob
import pandas as pd
import numpy as np
import time

import janitor

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-benefits-data")

# COMMAND ----------

def loadUOB_Fees_reftable():
    filename = f"{root_dir}/uob_fees_ref.xlsx"
    try:
        df_ref = pd.read_excel(filename, sheet_name=0, header=0, na_filter=False) 
    except:
        print(f"Reading {filename} failed.")
        pass
    
    df_ref = cf.strip_clean_drop(df_ref)
    df_ref = df_ref.rename(columns={"name": "fees"})
    #df_ref['fees'] = df_ref['fees'].str.lower()

    df_ref = (
            df_ref.clean_names(axis=None,column_names='fees')
        ) # using pyjanitor 

    df_ref.info()
    return df_ref

ref_df = loadUOB_Fees_reftable()
ref_df.fees.value_counts()

# COMMAND ----------

def readCreditNote():
    dflist = []
    filenames = "*UOB Credit Note*.xlsx"
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"{file}, {fileCreationDate}")

        try:
            datafile = pd.read_excel(file, sheet_name=0, header=0)
        except:
            print(f"Reading {file} failed.")
            pass
        datafile = cf.strip_clean_drop(datafile)
        datafile['filename'] = os.path.basename(file)
        # set dtypes correctly
        """datafile["departure_date"] = pd.to_datetime(datafile["departure_date"], dayfirst=True, errors="coerce").dt.date
        datafile["return_date"] = pd.to_datetime(datafile["return_date"], dayfirst=True, errors="coerce").dt.date
        datafile["invoice_date"] = pd.to_datetime(datafile["invoice_date"], dayfirst=True, errors="coerce").dt.date
        datafile["booking_date"] = pd.to_datetime(datafile["booking_date"], dayfirst=True, errors="coerce").dt.date"""

        dflist.append(datafile)
    
    df = pd.concat(dflist, ignore_index=True)

    print(df.info())
    return df

uobcr_df = readCreditNote()
uobcr_df

# COMMAND ----------

def readUOBTOP():
    dflist = []
    filenames = "*UOBTP*.xlsx"
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        print(f"{file}, {fileCreationDate}")

        try:
            datafile = pd.read_excel(file, sheet_name=0, header=2)
        except:
            print(f"Reading {file} failed.")
            pass
        datafile = cf.strip_clean_drop(datafile)
        datafile['filename'] = os.path.basename(file)
        # set dtypes correctly
        datafile["departure_date"] = pd.to_datetime(datafile["departure_date"], dayfirst=True, errors="coerce").dt.date
        datafile["return_date"] = pd.to_datetime(datafile["return_date"], dayfirst=True, errors="coerce").dt.date
        datafile["invoice_date"] = pd.to_datetime(datafile["invoice_date"], dayfirst=True, errors="coerce").dt.date
        datafile["booking_date"] = pd.to_datetime(datafile["booking_date"], dayfirst=True, errors="coerce").dt.date

        dflist.append(datafile)
    
    df = pd.concat(dflist, ignore_index=True)

    print(df.info())
    return df

# COMMAND ----------

uobtp_df = readUOBTOP()

# COMMAND ----------

uobtp_df['itinerary_last3'] = uobtp_df.itinerary.str.split('/').str[-1]
uobtp_df['origin'] = uobtp_df.itinerary.str.split('/').str[0]
uobtp_df['one_way'] = uobtp_df['itinerary_last3'] == uobtp_df['sector']


# COMMAND ----------

#uobtp_df.query('invoice_no in [805291,866475] or pnr == "1S8R3J"')

# COMMAND ----------

# melt columns into rows

# list of columns to be converted
fees = ['air_amount', 'air_taxes', 'transfee', 'online_transaction_fee', 'financial_fee', 'refund_servicing_fee', 'courier_fee', 'visacharges', 'visa_handling_fees', 'car_tran', 'car_handling_fee', 'insurance', 'etc', 'etc_ticket_issuance_charge', 'reissue_fee', 'seat_purchase_fee', 'misc', 'gst', 'hotel_amount', 'car_amount', 'other_fees', 'transaction_fee', 're_issue_fee', 'financial_charge', '24_hours_answering_service_charge', 'agent_refund_fee', 'online_booking_tool_transaction_fee', 'internet_transaction_fee', 'car_transfer', '24_hours_ticket_issuance_charge', 're_validate_fee', 'airline_admin_fee', 'baggage_handling_fee', 'baggage_purchase_fee']

# columns that will not be melted
id_vars = ['invoice_no', 'invoice_date', 'booking_date', 'pnr', 'traveller_name', 'employee_id', 'department', 'cost_centre', 'trip_purpose', 'trip_approval_code', 'ticket_no', 'departure_date', 'return_date', 'itinerary', 'mileage', 'al', 'airline', 'sector', 'destination', 'cl', 'class', 'booking_type', 'invoice_amount', 'fees_subtotal', 'hotel_room_nights', 'customer_code', 'customer_name', 'filename', 'designation', 'itinerary_last3','one_way','origin']

# melt the dataframe multiple times and assign new column names
uobtp_df_long = uobtp_df.melt(id_vars=id_vars, value_vars=fees, var_name='fees', value_name='fee_amount')
uobtp_df_long['fee_amount'] = uobtp_df_long['fee_amount'].fillna(0)
uobtp_df_long.head(2)


# COMMAND ----------

uobtp_df_long.info()

# COMMAND ----------

db.insert_with_progress(uobtp_df_long,"tbl_uobtravel_detail",engine)

# COMMAND ----------

db.insert_with_progress(ref_df,"ref_uobtravel_fees",engine)

# COMMAND ----------


db.insert_with_progress(uobcr_df,"tbl_uob_creditnote",engine)

# COMMAND ----------


