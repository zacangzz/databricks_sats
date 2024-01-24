# Databricks notebook source
# essential imports
import sys
import os
import pandas as pd
import numpy as np

# import my packages
from pkg_all import cf
from pkg_all import db

# COMMAND ----------

# connect to SQL Server
engine = db.connect_SQLServer()

# COMMAND ----------

from sqlalchemy.orm import Session

# COMMAND ----------


# SQL query
sql_query = """
SELECT *
FROM tbl_hc_eelisting
WHERE CAST(eom AS DATE) = (SELECT MAX(CAST(eom AS DATE)) FROM tbl_hc_eelisting)
AND active = 'True'
AND source = 'SF Direct'
AND flexi = 'False'

UNION

SELECT *
FROM tbl_hc_eelisting
WHERE CAST(date_left AS DATE) >= '2022-01-01'
AND active = 'False'
AND source = 'SF Direct'
AND flexi = 'False'
"""

# Execute the query and fetch results
with Session(engine) as session:
    result = session.execute(sql_query)
    rows = result.fetchall()

# Convert to a list of dictionaries
data = [dict(row) for row in rows]

# Create a Pandas DataFrame
dataset = pd.DataFrame(data)
dataset.info()

# COMMAND ----------

dataset = cf.strip_clean_drop(dataset)
dataset.info()

# COMMAND ----------

# feature engineering
dataset['local'] = False
dataset.loc[(dataset['fund_type'] == 'Central Provident Fund') | 
               (dataset['fund_type'] == 'Scheme for Permanent Resident'), 'local'] = True

# COMMAND ----------

# pre-process and selection
mask = ['active','age','local','industry','business','business_unit','parent_department','grade','job_grade','on_shift','marital_status','gender_key','nationality','employee_group','job']
data_selection = dataset[mask].copy()
data_selection['active'] = data_selection['active'].astype(int)
data_selection['local'] = data_selection['local'].astype(int)
data_selection['on_shift'] = data_selection['on_shift'].astype(int)
data_selection['marital_status'] = data_selection['marital_status'].fillna('')
data_selection = data_selection.dropna(axis=0,how='any')
data_selection = data_selection.replace({pd.NA: np.nan})
data_selection.info()

# COMMAND ----------

data_selection.active.value_counts()

# COMMAND ----------

# create pyspark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# import parallel back-end
from pycaret.parallel import FugueBackend

# COMMAND ----------

from pycaret.classification import *
s = setup(
    data=data_selection,
    target='active',
    fix_imbalance=True,
    categorical_features = ['industry','business','business_unit','parent_department','grade','job_grade','marital_status','gender_key','nationality','employee_group','job']
    )
# compare models
best = compare_models(parallel = FugueBackend(spark))

# COMMAND ----------

# evaluate trained model
evaluate_model(best)

# COMMAND ----------

model_tuned = tune_model(best, choose_better = True)

# COMMAND ----------

interpret_model(model_tuned)

# COMMAND ----------

save_model(model_tuned, 'ml_model')

# COMMAND ----------

from pycaret.clustering import *
s_cluster = setup(data=data_selection, normalize = True, categorical_features = ['business_unit','parent_department','grade','job_grade','marital_status','gender_key','nationality'])
best_cluster = compare_models(parallel = FugueBackend(spark))
evaluate_model(best_cluster)

# COMMAND ----------


