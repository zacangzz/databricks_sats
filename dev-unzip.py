# Databricks notebook source
# essential imports
import os
import glob
import pandas as pd
import numpy as np
import time

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

!pip install pyzipper==0.3.6
import pyzipper

filenames = "*.zip"
for file in glob.glob(f"{root_dir}/{filenames}"):
    fileCreationDate = time.ctime(os.path.getctime(file))
    print(f"{file}, {fileCreationDate}")

    zip_password = 'SatsOneTime123!'
    with pyzipper.AESZipFile(file) as zf:
        zf.pwd = zip_password.encode()
        zf.extractall(f"{root_dir}/")


# COMMAND ----------


