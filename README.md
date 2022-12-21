# PyCsvImport
A Python module for importing a CSV into an existing database table in MS SQL, PostgreSQL, MySQL, and MariaDB.

Usage
```python
from PyCsvImport import PyCsvImport


db_server = "localhost"
db_name = "my_database
conn = f"Driver={SQL Server};Server={db_server};Database={db_name};Trusted_Connection=True;"
table_schema = "dbo"
table_name = "my_table"
filepath = "data.csv"

importer = PyCsvImport()
_ = importer.execute_sql([f"TRUNCATE TABLE {table_schema}.{table_name}"], conn)
_ = await importer.from_file(filepath, table_schema, table_name, conn, ',')
```
