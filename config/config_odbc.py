# Databricks notebook source
# helper files to install ODBC driver: MSODBCSQL18

# COMMAND ----------

dbutils.fs.put("config2.sh","""
#!/bin/bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
""", True)
# for databricks runtime 13.3 ml, still works for runtime 14.2 ml.
# shell file for databricks compute runtime init


# COMMAND ----------

rdbutils.fs.put("dbfs:/databricks/scripts/install-msodbcsql18-ml13.sh","""
#!/bin/bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
""", True)
# for databricks runtime 13 ml

# COMMAND ----------

dbutils.fs.put("dbfs:/databricks/scripts/install-msodbcsql18.sh","""
#!/bin/bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
""", True)
# for databricks runtime 12.2 ml

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/microsoft.gpg >/dev/null
# MAGIC curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# COMMAND ----------


