# Databricks notebook source
# Import the packages from pkg_all
from pkg_all import cf
from pkg_all import db

# Other imports
import pandas as pd
import os

# Initialise DB engine
engine = db.connect_SQLServer()


# COMMAND ----------

from pyspark.sql import SparkSession

def read_sql_table(table, spark_session=None):
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
    
    if spark_session is None:
        spark_session = SparkSession.builder.getOrCreate()

    df = (spark_session.read
          .format("sqlserver")
          .option("host", hostname)
          .option("port", port)
          .option("user", username)
          .option("password", password)
          .option("database", database)
          .option("dbtable", table)
          .load()
         )
    return df

spark = SparkSession.builder.getOrCreate()
remote_table = read_sql_table("tbl_hc_eelisting", spark)


# COMMAND ----------

hostname = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
jdbc_url = f"jdbc:sqlserver://{hostname}:{port};databaseName={database}"

from pyspark.sql import SparkSession


# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Read data from SQL Server into Spark DataFrame
df = (spark.read
  .format("jdbc")
  .option("url", jdbc_url)
  .option("user", username)
  .option("password", password)
  .option("query", 'SELECT TOP 10 * FROM tbl_hc_eelisting')
  .load()
  )

import pyspark.pandas as ps
df1 = ps.read_sql('SELECT TOP 10 * FROM tbl_hc_eelisting',jdbc_url)
df1

# COMMAND ----------


# quick check on the sql server for headcount listing viability
sql_query = \
'''
SELECT
    eom,
    source,
    COUNT(DISTINCT persno) AS Headcount
FROM tbl_hc_eelisting
WHERE active = 'TRUE'
GROUP BY eom, source
'''
df2 = pd.read_sql(sql_query,engine)
df2.info()

# COMMAND ----------

display(df2)

# COMMAND ----------

# Initialise DB (DNA) engine
engine_dna = db.connect_SQLServer("DNA")

# COMMAND ----------

sql_query =  \
'''

SELECT c.*, e.tenure, e.pay_scale_group, e.active, e.eom, e.business_unit, e.business, e.industry, e.job_grade, e.personnel_subarea
FROM 
(
    SELECT DISTINCT persno, eom, total_monthly_basic
    FROM tbl_sf_sql_comp
) as c
LEFT JOIN 
(
    SELECT persno, eom, tenure, pay_scale_group, active, business_unit, business, industry, job_grade, personnel_subarea
    FROM tbl_hc_eelisting
    WHERE CAST(eom AS DATE) >= '2023-01-01'
    AND location IN ('SINGAPORE')
    AND source = 'SF Direct'
) as e
ON c.persno = e.persno AND c.eom = e.eom

'''
sql_query =  \
'''
SELECT persno, eom, tenure, pay_scale_group, active, business_unit, business, industry, personnel_subarea
FROM tbl_hc_eelisting
WHERE CAST(eom AS DATE) = '2023-08-31'
-- AND pay_scale_group IN ('H1')
AND job_grade IN ('MANAGERS TO AVP')
AND location IN ('SINGAPORE')
AND source = 'SF Direct'

'''

sql_query =  \
'''
SELECT *
-- FROM dna.ge_dept_detail_inactive
FROM dna.ge_dept_detail

'''

sql_query =  \
'''
SELECT
    leave_type_code,
    SUM (duration_days) AS leave_days_sum
FROM tbl_sf_sql_timeoff
GROUP BY leave_type_code

'''

sql_query =  \
'''
SELECT
    *
FROM tbl_hc_eelisting
WHERE CAST(date_joined AS DATE) >= '2023-04-01' AND CAST(date_joined AS DATE) <= '2024-03-31'

'''


# COMMAND ----------

sql_query =  \
'''
SELECT persno, job, grade, business_unit, parent_department, organizational_unit, todays_date, flexi
FROM tbl_hc_eelisting
WHERE job_grade = 'NON EXECUTIVE'
	AND business_unit = 'CARGO SERVICES'
	AND active = 'TRUE'
	AND CAST(todays_date AS DATE) = (SELECT MAX(CAST(todays_date AS DATE)) FROM tbl_hc_eelisting)
	AND flexi = 'FALSE'
'''
df2 = pd.read_sql(sql_query,engine)
df2.info()

# COMMAND ----------

df2.flexi.value_counts()

# COMMAND ----------

sql_query =  \
'''
SELECT
		t1.*
    FROM tbl_hc_eelisting AS t1
    INNER JOIN (
        SELECT
            persno,
            MIN(eom) AS min_eom
        FROM tbl_hc_eelisting
        WHERE industry = 'AVIATION'
            AND business_unit IN ('PAX SERVICES', 'APRON SERVICES', 'CARGO SERVICES', 'CATERING', 'GTRSG', 'SATS APS')
            AND job_grade = 'NON EXECUTIVE'
        -- WHERE CAST(date_joined AS DATE) >= '2023-04-01' AND CAST(date_joined AS DATE) <= '2024-03-31'
        GROUP BY persno
    ) AS t2 ON t1.persno = t2.persno AND t1.eom = t2.min_eom
'''

sql_query =  \
'''
SELECT
		t1.*
    FROM tbl_hc_eelisting AS t1
    INNER JOIN (
        SELECT
            persno,
            MIN(eom) AS min_eom
        FROM tbl_hc_eelisting
        -- WHERE CAST(date_joined AS DATE) >= '2023-04-01' AND CAST(date_joined AS DATE) <= '2024-03-31'
        GROUP BY persno
    ) AS t2 ON t1.persno = t2.persno AND t1.eom = t2.min_eom
'''

# COMMAND ----------

df2 = pd.read_sql(sql_query,engine)
df2.query('date_joined >= "2023-04-01" & date_joined <= "2024-03-31"').nunique()#to_csv("1735.csv")

# COMMAND ----------

df = pd.read_sql(sql_query,engine)
#df = df.sort_values(by=['persno','eom'])
#df = df.drop_duplicates(subset='persno',keep='last')
#df.head(8)
#df.to_csv('1745.csv')

# COMMAND ----------

df.persno.nunique()

# COMMAND ----------

df.groupby(['eom','personnel_subarea','pay_scale_group']).persno.nunique()

# COMMAND ----------

sql_query =  \
'''
SELECT TOP(100) *
FROM hc.tams_approvedOTClaims_v_t

'''

# COMMAND ----------

ot_df = pd.read_sql(sql_query,engine_dna)
ot_df.info()

# COMMAND ----------

ot_df.head()

# COMMAND ----------

df_etl_sf_listing = pd.read_sql(
    '''
    SELECT DISTINCT zip_code, country_zip_code FROM tbl_hc_eelisting
    WHERE country_zip_code LIKE 'Singapore'
    AND location IN ('SINGAPORE')
    AND source = 'SF Direct'
    '''
    ,engine
)
df_etl_sf_listing.info()

# COMMAND ----------

df_etl_sf_listing.zip_code.value_counts()

# COMMAND ----------


