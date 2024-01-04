__author__ = "Zac Ang"
__email__ = "zhaozong.ang@gmail.com"
"""
pkg_dbconnect.py
Database connection modules to help with CRUD operations. Connects to DB.
Also connects to Azure Blob Storage using Databricks Runtime functions.
"""

# db utils & databricks runtime imports for databricks
try:
    from databricks.sdk.runtime import *
except:
    pass
# from pyspark.dbutils import DBUtils

# other imports
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm
import logging
import pyodbc

load_dotenv()

def check_pyodbc_driver():
    # use the correct pyodbc driver version
    odbcDriver = ''
    drivers = pyodbc.drivers()
    drivers_needed = ['ODBC Driver 17 for SQL Server','ODBC Driver 18 for SQL Server']
    if len(drivers) == 1:
        odbcDriver = drivers[0]
        print(f'Using driver: {odbcDriver}.')
    else:
        for substring in drivers:
            if substring in drivers_needed:
                odbcDriver = substring
                print(f'Using driver: {odbcDriver}.')
    return odbcDriver

# connect to MS SQL Server - using SQLAlchemy
# choose which Server/DB to connect to
def connect_SQLServer(server: str = ""):
    prefix = "" # default connect to HC's SQL Server
    if server == "DNA":
        prefix = "DNA_" # connect to DNA's corporate database
    print(f'Connecting to: {prefix}')

    username = os.getenv(f'{prefix}DB_USERNAME')
    password = os.getenv(f'{prefix}DB_PASSWORD')
    host = os.getenv(f'{prefix}DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv(f'{prefix}DB_DATABASE')
    
    #print(username,host,database)

    url = "mssql+pyodbc://{user}:{passwd}@{host}:{port}/{db}?driver={dr}".format(
        user=username, passwd=password, host=host, port=port, db=database, dr=check_pyodbc_driver()
    )
    try:
        # establishing the connection to the database using engine as an interface
        engine = create_engine(url, fast_executemany=True, future=True)
        # wrapping connection using 'with' for compatibility with sqlalchemy 2.x
        with engine.connect() as conn:
            print(f"SQLAlchemy Engine Connected")
    except Exception as e:
        print(f"SQLAlchemy Engine Error: {e}")
        raise
    return engine

#connect_SQLServer("DNA")

def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))

def insert_with_progress(df, db, engine):
    # using sessionmaker compatibility with sqlalchemy 2.x
    Session = sessionmaker(bind=engine, future=True)
    chunksize = int(len(df) / 10)

    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunksize)):
            replace = "replace" if i == 0 else "append"

            with Session() as session:
                with session.begin():
                    cdf.to_sql(
                        db,
                        schema="dbo",
                        con=session.bind,
                        if_exists=replace,
                        index=False,
                        chunksize=10000,
                    )
                session.commit()
            pbar.update(chunksize)
            tqdm._instances.clear()

def connect_AzureBlob(container):
    # Connect to Azure Blob & mount blob container to DBFS locally within Databricks
    ## ids/keys/names etc. for access to Azure Blob
    storage_account_name = os.getenv('BLOB_ACCOUNT')
    blob_container = container #"hc-sf-data"
    secret_scope = os.getenv('BLOB_SECRETSCOPE')
    secret_key = os.getenv('BLOB_SECRETKEY')
    application_id = os.getenv('BLOB_APPID')
    directory_id = os.getenv('BLOB_DIRID')
    root_dir = "" # for the dbutils mount

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": application_id,
        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(
            scope=secret_scope, key=secret_key
        ),
        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"
        + directory_id
        + "/oauth2/token",
    }
    try:
        dbutils.fs.mount(
            source="abfss://"
            + blob_container
            + "@"
            + storage_account_name
            + ".dfs.core.windows.net/",
            mount_point="/mnt/" + blob_container,
            extra_configs=configs,
        )
    except:
        '''dbutils.fs.updateMount(
            source="abfss://"
            + blob_container
            + "@"
            + storage_account_name
            + ".dfs.core.windows.net/",
            mount_point="/mnt/" + blob_container,
            extra_configs=configs,
        )'''
        # dbutils.fs.unmount("/mnt/" + blob_container) #run this when secrets have changed!
        dbutils.fs.refreshMounts()

    azureTestFile = "AzureConnectionTest.txt"
    ## check if file is readable in blob
    def check_access(file, mode):
        try:
            open(file, mode)
            return True
        except PermissionError:
            print("ERROR")
            return False
    
    for i in range(0, 10):
        if check_access("/dbfs/mnt/" + blob_container + "/" + azureTestFile, "w") == False:
            time.sleep(2)
        else:
            root_dir = f"/dbfs/mnt/{blob_container}"
            break

    ##check if file exists in mount
    if check_access(f"{root_dir}/{azureTestFile}", "w") == False:
        raise Exception("File not found")

    print(root_dir)
    return root_dir

###