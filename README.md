# PyCsvImport
A Python module for importing a CSV into an existing database table in MS SQL, PostgreSQL, MySQL, and MariaDB.

Usage
```python
from PyCsvImport import PyCsvImport
import psycopg2

# PostgreSQL example.
get_connection = lambda: psycopg2.connect(
    database='db_name',
    host='localhost',
    user='uid',
    password='pwd',
    port='5432'
)
    
importer = PyCsvImport(connection=get_connection, column_name_qualifier='""')

table_schema = "dbo"
table_name = "my_table"
filepath = "data.csv"

_ = importer.execute_sql([f"TRUNCATE TABLE {table_schema}.{table_name}"])
# This method gets your table schema and formats the values in the CSV to the correct data type.
_ = await importer.from_file(filepath, table_schema, table_name)
```

Note: For MS SQL change the `column_name_qualifier = '""'` argument to `column_name_qualifier = '[]'`.
