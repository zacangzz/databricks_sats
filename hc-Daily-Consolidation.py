# Databricks notebook source
# essential imports
import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# commonfunc
import pkg_commonfunctions as cf
# connect to db
import pkg_dbconnect as db

# COMMAND ----------

engine = db.connect_SQLServer()

# COMMAND ----------

# essential scripts for standalone scripting
# get age based on date 
def calculateAge(x):
    if pd.isna(x['date_left']):
        today = x['eom']
    else:
        today = x['date_left']
    
    if pd.isna(x['birth_date']):
        return 0
    else:
        age = relativedelta(today, x['birth_date'])
        return int(age.years)

def calculate_age(row):
    today = row['eom'] if pd.isna(row['date_left']) else row['date_left']
    birth_date = row['birth_date']

    if pd.isna(birth_date):
        return 0

    age = relativedelta(today, birth_date)
    return age.years

def calculate_age(row):
    today = row['eom'] if pd.isna(row['date_left']) else row['date_left']
    birth_date = row['birth_date']

    if pd.isna(birth_date):
        return 0

    age = (today - birth_date).days / 365.25
    return int(age)

# get age based on date 
def calculateTenure(x):
    if pd.isna(x['date_left']):
        today = x['eom']
    else:
        today = x['date_left']
        
    if pd.isna(x['date_joined']):
        return pd.NA
    else:
        tenure = relativedelta(today, x['date_joined'])
        return round(tenure.years + (tenure.days/365),2)

def calculate_tenure(row):
    today = row['eom'] if pd.isna(row['date_left']) else row['date_left']
    date_joined = row['date_joined']

    if pd.isna(date_joined):
        return pd.NA

    tenure = (today - date_joined).days / 365.25
    return round(tenure, 2)

# COMMAND ----------

# get latest ref mapping tables
query_b = '''
SELECT * FROM tbl_reftable_grade
'''
ref_grade = pd.read_sql(
    query_b, engine
)
query_c = '''
SELECT * FROM tbl_reftable_bu
'''
ref_bu = pd.read_sql(
    query_c, engine
)
query_d = '''
SELECT * FROM tbl_reftable_company
'''
ref_company = pd.read_sql(
    query_d, engine
)
query_e = '''
SELECT * FROM tbl_reftable_attrition
'''
ref_attrition = pd.read_sql(
    query_e, engine
)
query_f = '''
SELECT * FROM tbl_reftable_jobgroup
'''
ref_jobgroup = pd.read_sql(
    query_f, engine
)

ref_grade.info()
ref_bu.info()
ref_company.info()
ref_attrition.info()
ref_jobgroup.info()


# COMMAND ----------

# get data from other ETL pipelines and combine it here to for a full, complete db.

# this is the old data prior up to Dec 2022
df_old_eelist = pd.read_sql(
    '''
    SELECT * FROM tbl_oldEeListing_toSQL
    '''
    ,engine
)
df_old_eelist['source'] = "Old"

# new non-SF data piped in on a regular basis
df_etl_non_sf_listing = pd.read_sql(
    '''
    SELECT * FROM tbl_nonsf_sql_monthly
    '''
    ,engine
)

# new SF direct data piped in on a regular basis, take only latest listing as at end of each month
df_etl_sf_listing = pd.read_sql(
    '''
    SELECT dataranked.* FROM
    (select *, RANK() over (partition by datetrunc(month, [todays_date]) order by [todays_date] desc) AS rank FROM tbl_sf_sql_daily) dataranked
    WHERE rank=1
    '''
    ,engine
)

# COMMAND ----------

# concat everything together
combined_df = pd.concat([df_old_eelist,df_etl_sf_listing,df_etl_non_sf_listing],ignore_index=True)
# clean up standardization for merge
combined_df['personnel_subarea'] = combined_df['personnel_subarea'].str.strip().str.upper()
combined_df['company_code'] = combined_df['company_code'].str.strip().str.upper()
combined_df['orgunit'] = combined_df['orgunit'].str.strip().str.upper()
combined_df['reason_for_action'] = combined_df['reason_for_action'].str.strip().str.upper()
combined_df['job'] = combined_df['job'].str.strip().str.upper()

# COMMAND ----------

combined_df.groupby(['eom','source']).persno.nunique()

# COMMAND ----------

# complete vlookup by merging
combined_df = pd.merge(combined_df, ref_grade, on="personnel_subarea",how='left')
combined_df = pd.merge(combined_df, ref_bu, on='orgunit',how='left', suffixes=(None, '_ref'))
combined_df = pd.merge(combined_df, ref_company, on='company_code',how='left')
combined_df = pd.merge(combined_df, ref_attrition, left_on='reason_for_action', right_on='attrition_reason',how='left')
combined_df = pd.merge(combined_df, ref_jobgroup, on='job',how='left')

# COMMAND ----------

combined_df['gender_key'] = combined_df['gender_key'].fillna('M')

# COMMAND ----------

# global transformations
combined_df['age'] = combined_df.apply(calculate_age, axis=1)
combined_df['age'] = combined_df['age'].astype(int,errors='ignore')

combined_df['tenure'] = combined_df.apply(calculate_tenure, axis=1)
combined_df['tenure'] = combined_df["tenure"].astype(float, errors="ignore")

# COMMAND ----------

# identify new joiners
combined_df = combined_df.sort_values(by=['persno','eom','active'],ascending=True)
combined_df['joiner_mark'] = combined_df.duplicated(subset=['persno','date_joined'],keep='first')
#combined_df['joiner_duplicated'] = False
#combined_df.loc[combined_df['joiner_mark'] == False, 'joiner_duplicated'] = True
#combined_df.drop('is_joiner',axis=1,inplace=True)
#combined_df['is_joiner'] = False
#combined_df.loc[combined_df['joiner_mark'] == False, 'is_joiner'] = True

# COMMAND ----------

combined_df.info()

# COMMAND ----------

# query validations:
q_leaversCount = combined_df.query("date_left >= '2022-04-01' & date_left <= '2023-03-31'  ").persno.nunique()
q_cohortLeavers = combined_df.query("date_joined >= '2019-04-01' & date_joined <= '2020-03-31' ").groupby('_dateleft_fy').agg('nunique')
q_headcount = combined_df.query("eom == '2021-03-31' & active==True ").persno.nunique()

# COMMAND ----------

# test to see why there are duplicates in the joiners list...
df1 = combined_df.query("date_joined >= '2022-04-01' & date_joined <= '2023-03-31' & joiner_mark==False")
df1[df1.duplicated(subset='persno',keep=False)]

# COMMAND ----------

combined_df.query("date_joined >= '2021-04-01' & date_joined <= '2022-03-31'").joiner_mark.value_counts()

# COMMAND ----------

combined_df.query("date_joined >= '2021-04-01' & date_joined <= '2022-03-31'")[combined_df.joiner_mark==False].persno.nunique()

# COMMAND ----------

combined_df.job_grade.value_counts()

# COMMAND ----------

print(combined_df.shape)
combined_df.info()

# COMMAND ----------

combined_df._datejoined_month.value_counts()

# COMMAND ----------

# load into database
db.insert_with_progress(combined_df,"tbl_hc_eelisting",engine)

# COMMAND ----------

# same ETL consolidation for Promotions
# get data from other ETL pipelines and combine it here to for a full, complete db.

# this is the old data prior up to Dec 2022
df_old_promolist = pd.read_sql(
    '''
    SELECT * FROM tbl_oldPromo_toSQL
    '''
    ,engine
)
df_old_promolist['source'] = "Old"

# new non-SF data piped in on a regular basis
df_etl_non_sf_listing_promo = pd.read_sql(
    '''
    SELECT * FROM tbl_nonsf_sql_monthly_promotions
    '''
    ,engine
)

# new SF direct data piped in on a regular basis, take only latest listing as at end of each month
df_etl_sf_listing_promo = pd.read_sql(
    '''
    SELECT dataranked.* FROM
    (select *, RANK() over (partition by datetrunc(month, [todays_date]) order by [todays_date] desc) AS rank FROM tbl_sf_sql_daily_promotions) dataranked
    WHERE rank=1
    '''
    ,engine
)

# COMMAND ----------

# concat everything together
combined_df_promo = pd.concat([df_old_promolist,df_etl_non_sf_listing_promo,df_etl_sf_listing_promo],ignore_index=True)

# COMMAND ----------

# load into database
db.insert_with_progress(combined_df_promo,"tbl_hc_eelisting_Promo",engine)

# COMMAND ----------

# ETL for Flexi/Temp Headcount info

# new SF direct data piped in on a regular basis, take only latest listing as at end of each month
df_etl_sf_flexi_temp = pd.read_sql(
    '''
    SELECT dataranked.* FROM
    (select *, RANK() over (partition by datetrunc(month, [todays_date]) order by [todays_date] desc) AS rank FROM tbl_sf_sql_daily_flexi_temp) dataranked
    WHERE rank=1
    '''
    ,engine
)

# COMMAND ----------

# clean up standardization for merge
df_etl_sf_flexi_temp['personnel_subarea'] = df_etl_sf_flexi_temp['personnel_subarea'].str.strip().str.upper()
df_etl_sf_flexi_temp['company_code'] = df_etl_sf_flexi_temp['company_code'].str.strip().str.upper()
df_etl_sf_flexi_temp['orgunit'] = df_etl_sf_flexi_temp['orgunit'].str.strip().str.upper()
df_etl_sf_flexi_temp['job'] = df_etl_sf_flexi_temp['job'].str.strip().str.upper()
# complete vlookup by merging
df_etl_sf_flexi_temp = pd.merge(df_etl_sf_flexi_temp, ref_grade, on="personnel_subarea",how='left')
df_etl_sf_flexi_temp = pd.merge(df_etl_sf_flexi_temp, ref_bu, on='orgunit',how='left', suffixes=(None, '_ref'))
df_etl_sf_flexi_temp = pd.merge(df_etl_sf_flexi_temp, ref_company, on='company_code',how='left')
df_etl_sf_flexi_temp = pd.merge(df_etl_sf_flexi_temp, ref_jobgroup, on='job',how='left')


# COMMAND ----------

db.insert_with_progress(df_etl_sf_flexi_temp,"tbl_hc_flexi_temp",engine)
