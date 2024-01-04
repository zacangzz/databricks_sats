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
from zoneinfo import ZoneInfo

# import my packages
from pkg_all import cf
from pkg_all import db
from pkg_all import em

# COMMAND ----------

import warnings
warnings.simplefilter("ignore", UserWarning)

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

# connect to Azure Blob
root_dir = db.connect_AzureBlob("hc-rec-tracker")

# COMMAND ----------

def readTrackerExcel():
    dflist = []
    filenames = "*.xlsx"

    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))

        if "Recruitment Status" not in file:
            # HCS Trackers
            try:
                print(f"Reading HCS Tracker: {file}, {fileCreationDate}")
                datafile = pd.read_excel(file,sheet_name = "SG-candidate info",header=0)
                datafile = cf.strip_clean_drop(datafile)
                # add an extra column to record file name
                datafile['filename']=os.path.basename(file)
                datafile['tracker_source']="HCS"
                #datafile.info()
            except:
                print(f"Reading {file} failed.")
                pass
            
        else:
            # TA Agency Trackers
            try:
                print(f"Reading TA Tracker: {file}, {fileCreationDate}")
                datafile = pd.read_excel(file,sheet_name = "Candidate info",header=0)
                datafile = cf.strip_clean_drop(datafile)
                # add an extra column to record file name
                datafile['filename']=os.path.basename(file)
                datafile['tracker_source']="TA"
                #datafile.info()
            except:
                print(f"Reading {file} failed.")
                pass

        # select only the necessary columns:
        select_mask = ['candidate_name', 'date_of_application_received', 'source', 'nationality', 'employment_type','position', 'bu', 'parent_department', 'job_grade', 'final_status', 'shortlisted_for_interview', 'reason_why_application_is_not_shortlisted_for_interview', 'date_of_interview', 'final_interview_outcome', 'date_of_interview_outcome_communication', 'reason_for_rejecting_candidate', 'offer_outcome', 'date_of_candidates_acceptance_rejection', 'reason_why_offer_is_rejected_by_candidate', 'join_date', 'staff_no', 'work_permit_status', 'wp_card', 'airport_pass', 'medical_report', 'staff_referral_fee','joining_bonus', 'filename', 'tracker_source']
        # drop useless columns
        valid_cols = [col for col in select_mask if col in datafile.columns]
        # Select only the valid columns
        datafile = datafile.loc[:, valid_cols]

        '''strings_to_drop = ['dose', 'booster', 'covid_19', 'vaccination', 'unnamed']
        drop_cols = [col for col in datafile.columns if any(substring in col for substring in strings_to_drop)]
        datafile = datafile.drop(drop_cols, axis=1)'''
        '''# remove excess text from column names
        strings_to_remove = ['_dd_mmm_yyyy', '_eg_19_may_2022', '_yyyy_mm_dd']
        datafile.rename(columns = lambda x: x.replace(strings_to_remove[0], '').replace(strings_to_remove[1], '').replace(strings_to_remove[2], ''), inplace=True)'''
        
        # drop nulls
        datafile = datafile.dropna(subset=['candidate_name'])
        datafile = cf.strip_clean_drop(datafile) # clean again
        print(datafile.columns.tolist())
        dflist.append(datafile)
    
    # bring all the dfs together & process them all
    df = pd.concat(dflist, ignore_index=True)
    
    # remove non-ascii characters
    df = df.applymap(cf.encode_strings)

    # fix date-time format, clean strings
    date_columns = ['join_date','date_of_application_received','date_of_interview','date_of_interview_outcome_communication','date_of_candidates_acceptance_rejection']
    for col in date_columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    return df

rectracker_df = readTrackerExcel()

# COMMAND ----------

rectracker_df.info()

# COMMAND ----------

'''# combine columns:
selected_cols = rectracker_df.filter(regex='^date_of_interview_if_column').columns
rectracker_df['date_of_interview'] = rectracker_df[selected_cols].apply(
    lambda row: ''.join(row.dropna().astype(str)), axis=1
)
rectracker_df = rectracker_df.drop(columns=selected_cols)
rectracker_df.info()

# combine columns:
selected_cols = rectracker_df.filter(regex='^reason_why_application_is_not_shortlisted_for_interview').columns
rectracker_df['reason_why_application_is_not_shortlisted_for_interview'] = rectracker_df[selected_cols].apply(
    lambda row: ''.join(row.dropna().astype(str)), axis=1
)
rectracker_df = rectracker_df.drop(columns=selected_cols)
rectracker_df.info()

# combine columns:
selected_cols = rectracker_df.filter(regex='^reason_for_rejecting_candidate').columns
rectracker_df['reason_for_rejecting_candidate'] = rectracker_df[selected_cols].apply(
    lambda row: ''.join(row.dropna().astype(str)), axis=1
)
rectracker_df = rectracker_df.drop(columns=selected_cols)
rectracker_df.info()'''

# COMMAND ----------

rectracker_df.date_of_interview

# COMMAND ----------

# insert time info
singapore_tz = ZoneInfo('Asia/Singapore')
singapore_time = datetime.now(singapore_tz)
singapore_time = singapore_time.replace(second=0, microsecond=0)
singapore_time = singapore_time.replace(tzinfo=None)

singapore_time
rectracker_df['last_update'] = singapore_time

# COMMAND ----------

singapore_time

# COMMAND ----------

rectracker_df.to_csv('tmp_rec_tracker.csv', index=False)

# COMMAND ----------

dataframe = pd.read_csv('tmp_rec_tracker.csv',na_filter=True)
dataframe = dataframe.replace(np.nan, '', regex=True)
# fix date-time format, clean strings
date_columns = ['join_date','date_of_application_received','date_of_interview','date_of_interview_outcome_communication','date_of_candidates_acceptance_rejection','last_update']
for col in dataframe.columns:
    if col in date_columns:
        dataframe[col] = dataframe[col].astype(str).str.strip()
        dataframe[col] = pd.to_datetime(dataframe[col], dayfirst=True, errors='coerce')
    else:
        dataframe[col] = dataframe[col].astype(str).str.strip().str.upper()
dataframe.info()

# COMMAND ----------

dataframe.sample(2)

# COMMAND ----------

# initialize error column
dataframe['error'] = ""

# Cat A
cols_A = ['candidate_name', 'date_of_application_received', 'nationality', 'bu', 
          'job_grade', 'parent_department', 'position', 'source', 
          'shortlisted_for_interview', 'employment_type']
dataframe.loc[dataframe[cols_A].isnull().any(axis=1), 'error'] += 'Cat A error; '

# Cat B
cols_B = ['date_of_interview', 'final_interview_outcome']
dataframe.loc[(dataframe['shortlisted_for_interview'] == "YES") & dataframe[cols_B].isnull().any(axis=1), 'error'] += 'Cat B error; '

# Cat C
cols_C = ['reason_why_application_is_not_shortlisted_for_interview']
dataframe.loc[(dataframe['shortlisted_for_interview'] == "NO") & dataframe[cols_C].isnull().any(axis=1), 'error'] += 'Cat C error; '

# Cat D
cols_D = ['date_of_interview_outcome_communication', 'offer_outcome']
dataframe.loc[(dataframe['final_interview_outcome'] == "SELECTED") & dataframe[cols_D].isnull().any(axis=1), 'error'] += 'Cat D error; '

# Cat E
cols_E = ['reason_for_rejecting_candidate']
dataframe.loc[(dataframe['final_interview_outcome'] == "REJECTED") & dataframe[cols_E].isnull().any(axis=1), 'error'] += 'Cat E error; '

# Cat F
cols_F = ['date_of_candidates_acceptance_rejection', 'join_date', 'final_status']
dataframe.loc[(dataframe['offer_outcome'] == "ACCEPTED") & dataframe[cols_F].isnull().any(axis=1), 'error'] += 'Cat F error; '

# Cat G
cols_G = ['date_of_candidates_acceptance_rejection', 'reason_why_offer_is_rejected_by_candidate']
dataframe.loc[(dataframe['offer_outcome'] == "REJECTED") & dataframe[cols_G].isnull().any(axis=1), 'error'] += 'Cat G error; '

# replace empty error strings with np.nan
dataframe['error'] = dataframe['error'].replace('', np.nan)

# COMMAND ----------

dataframe.info()

# COMMAND ----------

# setting up content for automated email blast
email_additional_text_pre = """
<p>This is an automated scheduled email sending you a list of all candidates with errors. Errors are classified into the following categories:</p>

<ul>
<li><b>Cat A:</b>
If any of the following columns are empty: [candidate_name],[date_of_application_received],[nationality],[bu],[job_grade],[parent_department],[position],[source],[shortlisted_for_interview],[employment_type]</li>
<li><b>Cat B:</b>
If [shortlisted_for_interview] == "YES", [date_of_interview], [final_interview_outcome] cannot be blank</li>
<li><b>Cat C:</b>
If [shortlisted_for_interview] == "NO", [reason_why_application_is_not_shortlisted_for_interview] cannot be blank</li>
<li><b>Cat D:</b>
If [final_interview_outcome] == "SELECTED", [date_of_interview_outcome_communication],[offer_outcome] cannot be blank</li>
<li><b>Cat E:</b>
If [final_interview_outcome] == "REJECTED", [reason_for_rejecting_candidate] cannot be blank</li>
<li><b>Cat F:</b>
If [offer_outcome] == "ACCEPTED", [date_of_candidates_acceptance_rejection],[join_date],[final_status] cannot be blank</li>
<li><b>Cat G:</b>
If [offer_outcome] == "REJECTED", [date_of_candidates_acceptance_rejection],[reason_why_offer_is_rejected_by_candidate] cannot be blank</li>
</ul>

"""
email_body = dataframe.groupby(['tracker_source','filename','error']).candidate_name.count().reset_index().to_html()
email_body = email_additional_text_pre + email_body
with open('data/email_body.html', 'w') as f:
    f.write(email_body)

# COMMAND ----------

dataframe[['filename','candidate_name','error']].query('error.notnull()').to_excel('data/errorlist.xlsx',index=False)

# COMMAND ----------

db.insert_with_progress(dataframe,"tbl_rec_trackers",engine)

# COMMAND ----------

os.remove("tmp_rec_tracker.csv")

# COMMAND ----------


