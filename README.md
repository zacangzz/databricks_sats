# databricks_sats

A set of Python Jupyter Notebooks on Databricks runtime designed for ETL: 
1. Extract data from Azure Blob Containers (automation achieve through email service & Sharepoint, with Power Automate + Logic Apps)
2. Transform data for consistency (data cleaning, basic feature engineering, etc)
3. Loading into SQL Database (cleaned dataset for further use, loading into Power BI, etc.)

## Runtime & Dependancies Info
Azure Databricks 13.0 ML (includes Apache Spark 3.4.0, Scala 2.12)  
Compute: Standard_E4as_v4, 32 GB Memory, 4 Cores  

Additional runtime libraries:
- com.microsoft.sqlserver:mssql-jdbc:11.2.3.jre18 (Maven)
- openpyxl==3.1.2 (PyPi)
- pyjanitor==0.24.0 (PyPi)
- python-dotenv==1.0.0 (PyPi)
- sqlalchemy==1.4.48 (PyPi)

#### Azure Storage Account
- Performance: Standard
- Replication: Locally-redundant storage (LRS)
- Account kind: StorageV2 (general purpose v2)

#### SQL Server 
- Service tier: Standard S0
- DTUs: 10 DTUs
- Max storage: 250 GB
- Storage redundancy: Locally-redundant backup storage
