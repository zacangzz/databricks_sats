# databricks_ETL
>Updated: Dec 2023 \
>Author: Zac Ang \
>Email: zhaozong.ang@gmail.com \

A set of Python Jupyter Notebooks on Databricks runtime designed for ETL: 
1. Extract data from Azure Blob Containers (automation achieve through email service & Sharepoint, with Power Automate + Logic Apps)
2. Transform data for consistency (data cleaning, basic feature engineering, EDA, etc)
3. Loading into SQL Database (cleaned dataset for further use, loading into Power BI, etc.)

## Description

**Folders**
- *config*: configuration scripts for Azure runtime
- *pkg_all*: standardised reusable code consistent across all notebooks, for direct import and usage

**Notebooks**
- prefix "prod": production level notebooks running various scripts for data processing
- prefix "dev": development level notebooks running test scenarios and scripts for reference and deployment when ready

## Runtime & Dependancies Info
Azure Databricks 14.2 ML (includes Apache Spark 3.5.0, Scala 2.12)
Compute: Standard_E4as_v4, 32 GB Memory, 4 Cores

Additional runtime libraries:
- com.microsoft.sqlserver:mssql-jdbc:11.2.3.jre18 (Maven)
- pandas==2.1.4 (PyPi)
- openpyxl==3.1.2 (PyPi)
- pyjanitor==0.26.0 (PyPi)
- python-dotenv==1.0.0 (PyPi)
- sqlalchemy==2.0.23 (PyPi)
- setuptools==69.0.2 (PyPi)

Single node, no isolation shared, using compute pool node type, init scripts within workspace, config.

#### Azure Storage Account
- Performance: Standard
- Replication: Locally-redundant storage (LRS)
- Account kind: StorageV2 (general purpose v2)

#### SQL Server 
- Service tier: Standard S0
- DTUs: 10 DTUs
- Max storage: 250 GB
- Storage redundancy: Locally-redundant backup storage

#### Note re: Azure Logic Apps
- In the 'Get files' function for MS Sharepoint, it is important to go into the settings and turn on pagination, otherwise the number of files returned will be limited to 100. Pagination comes with a threshold and it's important to set an appropriate threshold. Currently no clear detriment in setting it at the max allowable limit of 100,000.
