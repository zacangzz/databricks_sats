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
        
        # drop nulls
        datafile = datafile.dropna(subset=['candidate_name'])
        print(datafile.shape)
        dflist.append(datafile)
    
    # bring all the dfs together & process them all
    df = pd.concat(dflist, ignore_index=True)
    
    # remove non-ascii characters
    df = df.applymap(cf.encode_strings)

    # fix date-time format, clean strings
    date_columns = ['join_date','date_of_application_received','date_of_interview','date_of_interview_outcome_communication','date_of_candidates_acceptance_rejection']
    for col in date_columns:
        if col in df.columns:
            #df[col] = df[col].astype(str).str.strip()
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
        else:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df = df.replace(np.nan, '', regex=True)
    return df

rectracker_df = readTrackerExcel()

# COMMAND ----------

test = pd.read_excel("/dbfs/mnt/hc-rec-tracker/Recruitment Status Template_NTS_V Resource.xlsx",sheet_name = "Candidate info",header=0)
test = cf.strip_clean_drop(test)
select_mask = ['candidate_name', 'date_of_application_received', 'source', 'nationality', 'employment_type','position', 'bu', 'parent_department', 'job_grade', 'final_status', 'shortlisted_for_interview', 'reason_why_application_is_not_shortlisted_for_interview', 'date_of_interview', 'final_interview_outcome', 'date_of_interview_outcome_communication', 'reason_for_rejecting_candidate', 'offer_outcome', 'date_of_candidates_acceptance_rejection', 'reason_why_offer_is_rejected_by_candidate', 'join_date', 'staff_no', 'work_permit_status', 'wp_card', 'airport_pass', 'medical_report', 'staff_referral_fee','joining_bonus', 'filename', 'tracker_source']
# drop useless columns
valid_cols = [col for col in select_mask if col in test.columns]
# Select only the valid columns
test = test.loc[:, valid_cols]
test = test.dropna(subset=['candidate_name'])
test = test.applymap(cf.encode_strings)
print(test.columns)
date_columns = ['join_date','date_of_application_received','date_of_interview','date_of_interview_outcome_communication','date_of_candidates_acceptance_rejection']
for col in date_columns:
    if col in test.columns:
        print(col)
        #test[col] = test[col].astype(str).str.strip()
        test[col] = pd.to_datetime(test[col], dayfirst=True, errors='coerce')
    else:
        pass
test = test.replace(np.nan, '', regex=True)
test.info()

# COMMAND ----------

rectracker_df.query('filename=="Recruitment Status Template_NTS_V Resource.xlsx"').info()

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

#rectracker_df.to_csv('tmp_rec_tracker.csv', index=False, mode='w')

# COMMAND ----------

'''dataframe = pd.read_csv('tmp_rec_tracker.csv',na_filter=True)
# fix date-time format, clean strings
date_columns = ['join_date','date_of_application_received','date_of_interview','date_of_interview_outcome_communication','date_of_candidates_acceptance_rejection','last_update']
for col in dataframe.columns:
    if col in date_columns:
        #dataframe[col] = dataframe[col].astype(str).str.strip()
        dataframe[col] = pd.to_datetime(dataframe[col], dayfirst=True, errors='coerce')
    else:
        dataframe[col] = dataframe[col].astype(str).str.strip().str.upper()
dataframe.info()'''

# COMMAND ----------

# initialize error column
rectracker_df['error'] = ""

# Cat A
cols_A = ['candidate_name', 'date_of_application_received', 'nationality', 'bu', 
          'job_grade', 'parent_department', 'position', 'source', 
          'shortlisted_for_interview', 'employment_type']
rectracker_df.loc[rectracker_df[cols_A].isnull().any(axis=1), 'error'] += 'Cat A error; '

# Cat B
cols_B = ['date_of_interview', 'final_interview_outcome']
rectracker_df.loc[(rectracker_df['shortlisted_for_interview'] == "YES") & rectracker_df[cols_B].isnull().any(axis=1), 'error'] += 'Cat B error; '

# Cat C
cols_C = ['reason_why_application_is_not_shortlisted_for_interview']
rectracker_df.loc[(rectracker_df['shortlisted_for_interview'] == "NO") & rectracker_df[cols_C].isnull().any(axis=1), 'error'] += 'Cat C error; '

# Cat D
cols_D = ['date_of_interview_outcome_communication', 'offer_outcome']
rectracker_df.loc[(rectracker_df['final_interview_outcome'] == "SELECTED") & rectracker_df[cols_D].isnull().any(axis=1), 'error'] += 'Cat D error; '

# Cat E
cols_E = ['reason_for_rejecting_candidate']
rectracker_df.loc[(rectracker_df['final_interview_outcome'] == "REJECTED") & rectracker_df[cols_E].isnull().any(axis=1), 'error'] += 'Cat E error; '

# Cat F
cols_F = ['date_of_candidates_acceptance_rejection', 'final_status']
rectracker_df.loc[(rectracker_df['offer_outcome'] == "ACCEPTED") & rectracker_df[cols_F].isnull().any(axis=1), 'error'] += 'Cat F error; '

# Cat G
cols_G = ['date_of_candidates_acceptance_rejection', 'reason_why_offer_is_rejected_by_candidate']
rectracker_df.loc[(rectracker_df['offer_outcome'] == "REJECTED") & rectracker_df[cols_G].isnull().any(axis=1), 'error'] += 'Cat G error; '

# Cat H
cols_H = ['join_date']
rectracker_df.loc[
    ((rectracker_df['final_status'] == "JOINED") |
    (rectracker_df['final_status'] == "ACCEPTED OFFER AND PENDING TO JOIN") |
    (rectracker_df['final_status'] == "ACCEPTED OFFER AND PENDING FOR IPA/ENTRY APPROVAL") |
    (rectracker_df['final_status'] == "NO SHOW ON FIRST DAY"))
    & rectracker_df[cols_H].isnull().any(axis=1), 'error'
    ] += 'Cat H error; '

# replace empty error strings with np.nan
rectracker_df['error'] = rectracker_df['error'].replace('', np.nan)

# COMMAND ----------

rectracker_df.info()

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
If [offer_outcome] == "ACCEPTED", [date_of_candidates_acceptance_rejection],[final_status] cannot be blank</li>
<li><b>Cat G:</b>
If [offer_outcome] == "REJECTED", [date_of_candidates_acceptance_rejection],[reason_why_offer_is_rejected_by_candidate] cannot be blank</li>
<li><b>Cat H:</b>
If [final_status] == "JOINED" or "ACCEPTED OFFER AND PENDING TO JOIN" or "ACCEPTED OFFER AND PENDING FOR IPA/ENTRY APPROVAL" or "NO SHOW ON FIRST DAY", [join_date] cannot be blank</li>
</ul>

"""
email_body = rectracker_df.groupby(['tracker_source','filename','error']).candidate_name.count().reset_index().to_html()
email_body = email_additional_text_pre + email_body
with open('data/email_body.html', 'w') as f:
    f.write(email_body)

# COMMAND ----------

rectracker_df[['filename','candidate_name','error']].query('error.notnull()').to_excel('data/errorlist.xlsx',index=False)

# COMMAND ----------

def readTrackerExcel_interns():
    file = f"{root_dir}/Internship Recruitment Tracker.xlsx"
    try:
        datafile = pd.read_excel(file,sheet_name = "Master Tracker",header=0)
        datafile = cf.strip_clean_drop(datafile)
        # add an extra column to record file name
        datafile['filename']=os.path.basename(file)
        datafile['tracker_source']="HCS"
    except:
        print(f"Reading {file} failed.")
        pass

    # select only the necessary columns:
    select_mask = [
        'candidate_name', 'date_of_application_received', 'source', 'nationality', 'employment_type','position', 'bu', 'parent_department', 'job_grade', 'final_status', 'shortlisted_for_interview', 'reason_why_application_is_not_shortlisted_for_interview', 'date_of_interview', 'final_interview_outcome', 'date_of_interview_outcome_communication', 'reason_for_rejecting_candidate', 'offer_outcome', 'date_of_candidates_acceptance_rejection', 'reason_why_offer_is_rejected_by_candidate', 'join_date', 'staff_no', 'work_permit_status', 'wp_card', 'airport_pass', 'medical_report', 'staff_referral_fee','joining_bonus', 'filename', 'tracker_source', \
            'intake','school','course_of_study','start_date','end_date','recruitment_status' # only applicable for intern tracker
    ]
    # drop useless columns
    valid_cols = [col for col in select_mask if col in datafile.columns]
    # Select only the valid columns
    datafile = datafile.loc[:, valid_cols]

    # drop nulls
    datafile = datafile.dropna(subset=['candidate_name'])
    datafile = cf.strip_clean_drop(datafile) # clean again
    print(datafile.columns.tolist())

    # remove non-ascii characters
    datafile = datafile.applymap(cf.encode_strings)

    # add update time
    datafile['last_update'] = singapore_time

    # remove NAs
    datafile = datafile.replace(np.nan, '', regex=True)

    # fix date-time format, clean strings
    date_columns = ['join_date','date_of_application_received','date_of_interview','date_of_interview_outcome_communication','date_of_candidates_acceptance_rejection']
    for col in datafile.columns:
        if col in date_columns:
            #datafile[col] = datafile[col].astype(str).str.strip()
            datafile[col] = pd.to_datetime(datafile[col], dayfirst=True, errors='coerce')
        else:
            datafile[col] = datafile[col].astype(str).str.strip().str.upper()

    datafile['interns'] = True
    datafile.info()

    return datafile

rectracker_interns_df = readTrackerExcel_interns()

# COMMAND ----------

final_df = pd.concat([rectracker_df,rectracker_interns_df],ignore_index=True)

# COMMAND ----------

final_df.groupby(['source']).candidate_name.count()

# COMMAND ----------

db.insert_with_progress(final_df,"tbl_rec_trackers",engine)

# COMMAND ----------


