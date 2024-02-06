# Databricks notebook source
# this script helps to delete all items from the requested blob
import os
import glob
import time
# import my packages
from pkg_all import db

# COMMAND ----------

 def delete_all(container_name):
     # connect to Azure Blob
    root_dir = db.connect_AzureBlob(container_name)

    filenames = "*.*"
    for file in glob.glob(f"{root_dir}/{filenames}"):
        fileCreationDate = time.ctime(os.path.getctime(file))
        try:
            print(f"deleting file: {file}, {fileCreationDate}")
            os.remove(file)
            # dbutils.fs.rm(f'dbfs:/mnt/{root_dir}/{file}')
            print(f"{file} deleted.")
        except:
            print(f"Failed to delete {file}")

delete_all("hc-cb-info")

# COMMAND ----------


