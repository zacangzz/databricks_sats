# Databricks notebook source
# essential imports
import pandas as pd
import numpy as np
from datetime import datetime, date

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

engine = db.connect_SQLServer()

# COMMAND ----------

# get data from other ETL pipelines and combine it here to for a full, complete db.

# this is the old data
df_old_eelist = pd.read_sql(
    '''
    SELECT * FROM tbl_oldEeListing_toSQL_cleaned062023
    '''
    ,engine
)
df_old_eelist['source'] = "Old"

# current active ETL data piped in on a regular basis
df_etl_listing = pd.read_sql(
    '''
    SELECT * FROM tbl_hc_etl_current
    '''
    ,engine
)



# COMMAND ----------

# concat everything together
combined_df = pd.concat([df_old_eelist,df_etl_listing],ignore_index=True)
# clean up standardization for merge
combined_df['personnel_subarea'] = combined_df['personnel_subarea'].str.strip().str.upper()
combined_df['company_code'] = combined_df['company_code'].str.strip().str.upper()
combined_df['orgunit'] = combined_df['orgunit'].str.strip().str.upper()
combined_df['reason_for_action'] = combined_df['reason_for_action'].str.strip().str.upper()
combined_df['job'] = combined_df['job'].str.strip().str.upper()
combined_df['location'] = combined_df['location'].str.strip().str.upper()

# COMMAND ----------

# Use the 'isin' function to check if 'employee_group' is in 'flexi_groups' - marker to indicate flexi employees
flexi_groups = ['Temp-Regular', 'Flexi', 'Temporary staff', 'Student', 'External']
combined_df['flexi'] = combined_df['employee_group'].isin(flexi_groups)

# COMMAND ----------

combined_df.info()

# COMMAND ----------

combined_df.query('eom>="2022-04-01"').groupby(['eom','active']).persno.nunique()

# COMMAND ----------

'''# identify new joiners
combined_df = combined_df.sort_values(by=['persno','eom','active'],ascending=True)
combined_df['joiner_mark'] = ~combined_df.duplicated(subset=['persno','date_joined'],keep='first')'''


# COMMAND ----------

'''# Filter the dataframe where 'is_new_joiner' is True and 'date_joined' is between '2022-04-01' and '2023-03-31'
df_filtered = combined_df.query("date_joined >= '2022-04-01' & date_joined <= '2023-03-31' &joiner_mark == True")
# Find the duplicated rows by 'persno'
df_duplicated = df_filtered[df_filtered.duplicated(subset='persno', keep=False)]
# Now df_duplicated contains the duplicated rows by 'persno' where 'is_new_joiner' is True and 'date_joined' is between '2022-04-01' and '2023-03-31'
df_duplicated'''

# COMMAND ----------

q_2 = combined_df.query("date_joined >= '2022-04-01' & date_joined <= '2023-03-31' ").joiner_mark.sum()
q_3 = combined_df.query("date_joined >= '2022-04-01' & date_joined <= '2023-03-31' ").persno.nunique()

# COMMAND ----------

print(q_2)
print(q_3)

# COMMAND ----------

# compare month on month differences in headcount info

def diff_between_months(df, fields):
    df = df.sort_values(by='eom')

    diffs = []

    months = df['eom'].unique()
    for i in range(len(months) - 1):
        month_current = df[df['eom'] == months[i]]
        month_next = df[df['eom'] == months[i + 1]]

        for field in fields:
            # Merge on 'persno' and the field, keeping track of both old and new values for the field
            merged = pd.merge(month_current, month_next, on='persno', suffixes=('_old', '_new'))

            # If the field does not exist in either month_current or month_next, skip the rest of this loop
            if field + '_old' not in merged.columns or field + '_new' not in merged.columns:
                continue

            # additions
            added = merged.loc[merged[field + '_old'].isna() & merged[field + '_new'].notna()].copy()
            added['change'] = 'added'
            added['field'] = field
            added['eom'] = months[i+1]
            diffs.append(added)

            # deletions
            deleted = merged.loc[merged[field + '_new'].isna() & merged[field + '_old'].notna()].copy()
            deleted['change'] = 'deleted'
            deleted['field'] = field
            deleted['eom'] = months[i+1]
            diffs.append(deleted)

            # modifications
            modified = merged.loc[merged[field + '_old'].notna() & merged[field + '_new'].notna()].copy()
            modified = modified[modified[field + '_old'] != modified[field + '_new']]
            modified['change'] = 'modified'
            modified['field'] = field
            modified['eom'] = months[i+1]
            diffs.append(modified)

    # Concatenate all DataFrames
    diffs_df = pd.concat(diffs, ignore_index=True)

    return diffs_df


# COMMAND ----------

subset = combined_df.query("active==True")[['persno','orgunit','job','eom']]
subset.info()


# COMMAND ----------

combined_df_diff = diff_between_months(subset,['orgunit','job'])

# COMMAND ----------

combined_df_diff.info()

# COMMAND ----------

combined_df.info()

# COMMAND ----------

combined_df.orgunit.value_counts()

# COMMAND ----------

# query validations:
q_leaversCount = combined_df.query("date_left >= '2022-04-01' & date_left <= '2023-03-31'  ").persno.nunique()
q_cohortLeavers = combined_df.query("date_joined >= '2019-04-01' & date_joined <= '2020-03-31' ").groupby('_dateleft_fy').agg('nunique')
q_headcount = combined_df.query("eom == '2021-03-31' & active==True ").persno.nunique()

# COMMAND ----------

print(q_leaversCount)
#print(q_cohortLeavers)
print(q_headcount)

# COMMAND ----------

# load into database - differences listing
db.insert_with_progress(combined_df_diff,"tbl_hc_eelisting_difflist",engine)


# COMMAND ----------

# load into database
db.insert_with_progress(combined_df,"tbl_hc_eelisting",engine)

# COMMAND ----------


