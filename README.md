# PyCsvImport
A Python module for importing a CSV into an existing database table in MSSQL, PostgreSQL, MySQL, and MariaDB.

Usage
```python
import os
from typing import Dict, List
from PyCsvImport import PyCsvImport
import pyodbc
from models.ImportConfig import ImportConfig
from models.CsvImportConfig import CsvImportConfig
from models.DatabaseConfig import DatabaseConfig
from models.FileConfig import FileConfig
from lib.common import get_file, get_json

    
def get_connection_string(cfg:DatabaseConfig, username:str, password:str) -> str:
    return f"""
    Driver={{SQL Server}};
    Server={cfg.server};
    Database={cfg.dbname};
    Trusted_Connection={cfg.is_trusted};
    UID={username};
    PWD={password};
    """
    
if __name__ == '__main__':
    ROOT:str = os.curdir
    # Set config vars here.
    env:str = 'prod' # local|dev|staging|prod
    create_sql_insert_files:bool = False
    batch_size:int = 1000
    config_path:str = f"{ROOT}/env/{env}.json"
    host_config:ImportConfig = ImportConfig.from_json(config_path)
    csv_import_config:CsvImportConfig = host_config.csv_import_config
    database_config:DatabaseConfig = host_config.database_config
    files:List[FileConfig] = csv_import_config.files 
    csv_dir:str = csv_import_config.csv_dir
    db_credentials:Dict = get_json(f"{ROOT}/credentials/sql_server_user.json")
    cs:str = get_connection_string(
        database_config
        , db_credentials[env]['username']
        , db_credentials[env]['password']
    )
    get_connection = lambda: pyodbc.connect(cs)
    # Import CSV files.
    importer = PyCsvImport(connection=get_connection, column_name_qualifier='[]', batch_size=batch_size)
    importer.debug = True
    
    for i, cfg in enumerate(files):
        if not cfg.include:
            continue
        tablename = f"{cfg.table_schema}.{cfg.table_name}"
        print(f"Importing data to {tablename}...")
        filepath = f"{ROOT}/{csv_dir}/{cfg.source}"
        
        if cfg.truncate:
            importer.execute_sql([f"TRUNCATE TABLE {tablename}"])
        
        results = importer.from_file(
            filepath
            , table_schema=cfg.table_schema
            , table_name=cfg.table_name
            , delimiter=cfg.delimiter
            , sql_only=create_sql_insert_files)
        
        if create_sql_insert_files:
            with open(f"./sql/{tablename}-{file_count}.sql", "w") as f:
                f.write("\n".join(results))
    # for
# if
```

Note: For PostgreSQL change the `column_name_qualifier = '[]'` argument to `column_name_qualifier = '""'`.
