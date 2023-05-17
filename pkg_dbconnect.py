# db imports
from databricks.sdk.runtime import *
from pyspark.dbutils import DBUtils
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, InterfaceError
from tqdm import tqdm
from databricks.sdk.runtime import *

# load confi secrets from .env
load_dotenv()

# connect to MS SQL Server - HC Prod SATS DB, using SQLAlchemy
def connect_SQLServer():
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_DATABASE')
    
    #driver = "ODBC+Driver+17+for+SQL+Server"
    driver = os.getenv('DB_DRIVER')
    url = "mssql+pyodbc://{user}:{passwd}@{host}:{port}/{db}?driver={dr}".format(
        user=username, passwd=password, host=host, port=port, db=database, dr=driver
    )
    try:
        # establishing the connection to the database using engine as an interface
        engine = create_engine(url, fast_executemany=True)
        engine.connect()
        print(f"SQLAlchemy Engine Connected")
    except Exception as e:
        print("SQLAlchemy Engine Error")
    return engine

##connect_SQLServer()

def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))

def insert_with_progress(df, db, e):
    con = e.connect()
    chunksize = int(len(df) / 10)
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunksize)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(
                db,
                schema="dbo",
                con=e,
                if_exists=replace,
                index=False,
                chunksize=10000,
            )
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
        dbutils.fs.refreshMounts()

    azureTestFile = "AzureConnectionTest.txt"
    ##check if file is readable in blob
    def check_access(file, mode):
        try:
            open(file, mode)
            return True
        except PermissionError:
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