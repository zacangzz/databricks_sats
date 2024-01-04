# Databricks notebook source
# essential imports
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from datetime import datetime, date
import os

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# function using Spark to read SQL
def read_sql(table_or_query, spark_session=None, is_query=False):
    """
    Reads a table from SQL Server into a Spark DataFrame.

    Parameters:
    hostname (str): The hostname of the SQL Server.
    port (str): The port of the SQL Server. Default port is 1433.
    username (str): The username for the SQL Server.
    password (str): The password for the SQL Server.
    database (str): The name of the database.
    table (str): The name of the table (including schema if applicable).
    spark_session (SparkSession, optional): An existing SparkSession instance.

    Returns:
    DataFrame: A Spark DataFrame containing the data from the specified SQL Server table.
    """
    hostname = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    jdbc_url = f"jdbc:sqlserver://{hostname}:{port};databaseName={database}"
    
    if spark_session is None:
        spark_session = SparkSession.builder.getOrCreate()

    if is_query:
        # Execute the SQL query
        df = (spark_session.read
              .format("jdbc")
              .option("url", jdbc_url)
              .option("user", username)
              .option("password", password)
              .option("query", table_or_query)
              .load()
              )
    else:
        # Read the entire table
        df = (spark_session.read
              .format("jdbc")
              .option("url", jdbc_url)
              .option("dbtable", table_or_query)
              .option("user", username)
              .option("password", password)
              .load()
              )

    # Convert & return a PySpark Pandas dataframe
    pdf = ps.DataFrame(df)
    
    return pdf


# COMMAND ----------

# get latest ref mapping tables

ref_grade = read_sql('tbl_reftable_grade')
ref_bu = read_sql('tbl_reftable_bu')
ref_company = read_sql('tbl_reftable_company')
ref_attrition = read_sql('tbl_reftable_attrition')
ref_jobgroup = read_sql('tbl_reftable_jobgroup')

ref_grade.info()
ref_bu.info()
ref_company.info()
ref_attrition.info()
ref_jobgroup.info()


# COMMAND ----------

# get data from other ETL pipelines and combine it here. this combines only the current active ETLs, (excludes data from source = Old)

# new non-SF data piped in on a regular basis
df_etl_non_sf_listing = read_sql('tbl_nonsf_sql_monthly')

# new SF direct data piped in on a regular basis, take only latest listing as at end of each month
df_etl_sf_listing = read_sql(
    '''
    SELECT dataranked.* FROM
    (select *, RANK() over (partition by datetrunc(month, [todays_date]) order by [todays_date] desc) AS rank FROM tbl_sf_sql_daily) dataranked
    WHERE rank=1
    '''
    ,is_query=True
)


# COMMAND ----------

# concat everything together
combined_df = ps.concat([df_etl_sf_listing,df_etl_non_sf_listing],ignore_index=True)

# clean up standardization for merge
combined_df['personnel_subarea'] = combined_df['personnel_subarea'].str.strip().str.upper()
combined_df['company_code'] = combined_df['company_code'].str.strip().str.upper()
combined_df['orgunit'] = combined_df['orgunit'].str.strip().str.upper()
combined_df['reason_for_action'] = combined_df['reason_for_action'].str.strip().str.upper()
combined_df['job'] = combined_df['job'].str.strip().str.upper()
# complete vlookup by merging
combined_df = ps.merge(combined_df, ref_grade, on="personnel_subarea",how='left')
combined_df = ps.merge(combined_df, ref_bu, on='orgunit',how='left',suffixes=('', '_ref'))
combined_df = ps.merge(combined_df, ref_company, on='company_code',how='left')
combined_df = ps.merge(combined_df, ref_attrition, left_on='reason_for_action', right_on='attrition_reason',how='left')
combined_df = ps.merge(combined_df, ref_jobgroup, on='job',how='left')

#combined_df = pd.merge(combined_df, ref_bu, on='orgunit',how='left', suffixes=(None, '_ref'))
# fill NAs
combined_df['gender_key'] = combined_df['gender_key'].fillna('M')

'''# global transformations
combined_df = calculate_age(combined_df)
combined_df = calculate_tenure(combined_df)'''


# COMMAND ----------

combined_df.info()
# load into database
# db.insert_with_progress(combined_df,"tbl_hc_etl_current",engine)

# COMMAND ----------

# essential scripts for standalone scripting
# get age based on date
def calculate_age(df):
    today = df['eom'].where(df['date_left'].isna(), df['date_left'])
    birth_date = df['birth_date']

    df['age'] = ((today - birth_date).dt.days / 365.25).fillna(0).astype(int)
    df.loc[birth_date.isna(), 'age'] = 0

    return df

# get tenure/length of service
def calculate_tenure(df):
    today = df['eom'].where(df['date_left'].isna(), df['date_left'])
    date_joined = df['date_joined']

    df['tenure'] = (today - date_joined).dt.days / 365.25
    df['tenure'] = df['tenure'].round(2)
    df.loc[date_joined.isna(), 'tenure'] = pd.NA

    return df


# COMMAND ----------

# get latest ref mapping tables

ref_grade = pd.read_sql('SELECT * FROM tbl_reftable_grade', engine)
ref_bu = pd.read_sql('SELECT * FROM tbl_reftable_bu', engine)
ref_company = pd.read_sql('SELECT * FROM tbl_reftable_company', engine)
ref_attrition = pd.read_sql('SELECT * FROM tbl_reftable_attrition', engine)
ref_jobgroup = pd.read_sql('SELECT * FROM tbl_reftable_jobgroup', engine)

ref_grade.info()
ref_bu.info()
ref_company.info()
ref_attrition.info()
ref_jobgroup.info()


# COMMAND ----------

# get data from other ETL pipelines and combine it here. this combines only the current active ETLs, (excludes data from source = Old)

'''# this is the old data prior up to Dec 2022
df_old_eelist = pd.read_sql('SELECT * FROM tbl_oldEeListing_toSQL',engine)
df_old_eelist['source'] = "Old"'''

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

# load into database
db.insert_with_progress(combined_df,"tbl_hc_etl_current",engine)

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
