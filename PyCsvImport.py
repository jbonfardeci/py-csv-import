from typing import List, Dict, Tuple, Any
import pyodbc
import csv
import re

class ColumnSchema:
    column_name: str = None
    data_type: str = None
    
    def __init__(self, column_name, data_type):
        self.column_name = column_name
        self.data_type = data_type
        

class PyCsvImport:
    """
    Imports CSV files into Microsoft SQL Server with Pyodbc in batches.
    Features the cleaning and conversion of values to the correct data type 
        defined in the SQL Server destination table schema.
    """
    
    __commands = []
    __count = 0
    __batch_size = 100
    debug = False
    
    def __init__(self, batch_size:int=100):
        self.__batch_size = batch_size
    
    async def get_table_schema(self, table_name:str, table_schema:str, connection_string:str) -> List[ColumnSchema]:
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{table_schema}'
        ORDER BY ORDINAL_POSITION
        """
        table:List = await self.select_from_sql_server(sql, connection_string)
        schema = []
        for row in table:
            col_name, dtype = row
            schema.append(ColumnSchema(col_name, dtype))
        return schema
    
    async def select_from_sql_server(self, sql:str, connection_string:str) -> List:
        table = []
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            for row in cursor.fetchall():
                table.append(row)
        except Exception as ex:     
            raise ex
        finally:
            cursor.close()
            conn.close()
        return table

    def execute_sql(self, commands: List, connection_string: str) -> None:
        print(f"Executing {len(commands)} commands...")
        batch = ';\n'.join(commands)
        conn = pyodbc.connect(connection_string)
        conn.autocommit = True
        conn.timeout = 0
        cursor = conn.cursor()
        try:
            cursor.execute(batch)
            conn.commit()
        except Exception as ex:
            if self.debug:
                with open('./logs/log.sql', 'w', encoding='utf-8') as f:
                    f.write(batch)
            raise ex
        finally:
            cursor.close()
            conn.close()
        print(f"{len(commands)} commands executed.")
    
    def execute_batch(self, sql: str, connection_string: str) -> None:
        commands:List = self.__commands
        commands.append(sql)  
        if len(commands) < self.__batch_size:
            return
        self.execute_sql(commands, connection_string)
        self.__count += len(commands)
        commands.clear()
        print(f"Executed {self.__count} commands.")
        
    def build_sql_insert(self, tablename: str, col_names: List, values: List, schema: List[ColumnSchema]) -> str:
        if len(col_names) != len(values):
            raise Exception(f"The column names and values must have the same length. Error creating SQL for table '{tablename}'.")
        vals = []
        cols = []
        for col, val in zip(col_names, values):
            dtype:str = self.get_dtype(col, schema)
            if dtype != None:
                cols.append(col)
                vals.append( str(self.__convert_to_type(val, dtype)) )
            # if
        # for
        col_names_str = "[" + "], [".join(cols) + "]"
        values_str = ", ".join(vals)
        sql = f"INSERT INTO {tablename}({col_names_str})VALUES({values_str})"
        return sql

    def get_dtype(self, col_name:str, schema: List[ColumnSchema]) -> Any:
        results = list(filter(lambda o: o.column_name == col_name, schema))
        if len(results) > 0:
            return results[0].data_type
        return None
 
    async def from_file(self, filepath: str, table_schema: str, table_name: str, connection_string: str, delimiter=',', sql_only: bool=False) -> List[str]:
        """Import CSV from file.

        Args:
            filepath (str): 
            table_schema (str): 
            table_name (str): 
            connection_string (str): 
            delimiter (str, optional): Defaults to ','.

        Returns:
            bool: True if successful, False otherwise.
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            return await self.from_string(content, table_schema, table_name, connection_string, delimiter, sql_only)
    
    async def from_string(self, content:str, table_schema: str, table_name:str, connection_string:str, delimiter=',', sql_only: bool=False) -> List[str]:
        """Import from string.

        Args:
            content (str): 
            table_schema (str): 
            table_name (str): 
            connection_string (str): 
            delimiter (str, optional): Defaults to ','.

        Returns:
            bool: True if successful, False otherwise.
        """
        _list = content.splitlines()
        return await self.from_list(_list, table_schema, table_name, connection_string, delimiter, sql_only)
    
    async def from_list(self, _list: List[str], table_schema: str, table_name: str, connection_string: str, delimiter=',', sql_only: bool=False) -> List[str]:
        """Import from list of strings.

        Args:
            _list (List[str]): 
            table_schema (str): 
            table_name (str): 
            connection_string (str): 
            delimiter (str, optional): Defaults to ','.
            sql_only: bool=False - set to True if you only want to create an SQL statement w/o writing to the database.

        Returns:
            str
        """
        sql_commands = []
        tablename = f"{table_schema}.{table_name}"
        schema: List[ColumnSchema] = await self.get_table_schema(table_name, table_schema, connection_string)
        schema_cols = [col.column_name for col in schema]
        rdr = csv.reader(_list, delimiter=delimiter)
        # Get and clean the column names from the first row.
        col_names = list(map(self.__clean_str, next(rdr)))
        # Verify that all CSV column names are in the schema from the SQL Server table.
        # If the column names are not in the schema, remove them.
        missing_col_indexes = [ix for ix in range(len(col_names)) if col_names[ix] not in schema_cols]
        col_names = [col_names[ix] for ix in range(len(col_names)) if ix not in missing_col_indexes]
                
        # Do we want to raise an exception of the schema changed or ignore? Answer pending as of 10/21/2022.
        #if len(missing_col_indexes) > 0:
            #raise Exception(f"There was a schema change in table '{tablename}'")        
        
        # Get the row values from all subsequent rows.
        for row in rdr:
            # Clean row values.
            values = list(map(self.__clean_str, row))       
            # Remove any column values not in the table schema.
            write_values = [values[ix] for ix in range(len(values)) if ix not in missing_col_indexes]
                    
            # Skip malformed rows in CSV.
            if len(write_values) == len(col_names): 
                sql = self.build_sql_insert(tablename, col_names, write_values, schema)
                if sql_only:
                    sql_commands.append(sql)
                else:
                    self.execute_batch(sql, connection_string)
            # if
        # for
        return sql_commands
           
    async def create_sql_from_string(self, content:str, out_filepath:str, table_schema:str, table_name:str, connection_string:str, delimiter=','):
        """
        Outputs a SQL INSERT statement file.
        Args:
            content (str): string with multiple lines of delimited text
            out_filepath (str): output filepath
            table_schema (str): the table schema
            table_name (str): the table name
            connection_string (str): sql server connection string
        """
        await self.create_sql_from_list(content.splitlines(), out_filepath, table_schema, table_name, connection_string, delimiter)
    
    async def create_sql_from_list(self, _list: List[str], out_filepath:str, table_schema:str, table_name:str, connection_string:str, delimiter=','):
        """
        Outputs a SQL INSERT statement file.
        Args:
            _list (List[str]): 
            out_filepath (str): output filepath
            table_schema (str): the table schema
            table_name (str): the table name
            connection_string (str): sql server connection string
        """
        sql_commands = self.from_list(_list, table_schema, table_name, connection_string, delimiter, sql_only=True)
        sql = ";\n".join(sql_commands)
        with open(out_filepath, 'w', encoding='utf-8') as f:
            f.writelines(sql)
           
    async def create_sql_insert_file(self, filepath:str, out_filepath:str, table_schema:str, table_name:str, connection_string:str, delimiter=',') -> None:
        """
        Outputs a SQL INSERT statement file.
        Args:
            filepath (str): source filepath
            out_filepath (str): output filepath
            table_schema (str): the table schema
            table_name (str): the table name
            connection_string (str): sql server connection string
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            await self.create_sql_from_string(content, out_filepath, table_schema, table_name, connection_string, delimiter)

    #region private methods
    def __quote_string(self, s:Any) -> Any:
        return f"'{s}'"

    def __clean_str(self, s:str) -> str:
        return re.sub(r'([^\x00-\x7F]|\t|\n|\r\n|"|\')+', '', str(s))
    
    def __is_empty(self, s:str) -> bool:
        if s is None:
            return True
        return len(str(s).strip()) == 0

    def __convert_to_bit(self, val:str) -> int:
        if len(re.findall(r'(yes|1|true)', val, re.IGNORECASE)) > 0:
            return 1
        return 0

    def __convert_utc_date_to_iso(self, s:str) -> str:
        # Convert '2022-03-14 13:23:15 UTC' to '2022-03-14T13:23:15'
        s = re.sub(r'\sUTC$', '', s)
        s = re.sub(r'\s', 'T', s)
        return self.__quote_string(s)

    def __convert_to_numeric(self, val:str, dtype:str) -> Any:
        val = re.sub(r'[^0-9\.\-]', '', str(val))
        if self.__is_empty(val):
            return 'NULL'
        if dtype == 'int':
            val = val.split('.')[0]
            return int(val)
        return float(val)

    def __convert_to_type(self, val:str, dtype:str) -> Any:
        dtype = str(dtype).lower()
        val = str(val).strip()
        if self.__is_empty(val):
            return 'NULL'
        if dtype in ['date', 'datetime']:
            return self.__convert_utc_date_to_iso(val)
        if dtype == 'bit':
            return self.__convert_to_bit(val)
        if dtype in ['int', 'float', 'decimal', 'real', 'money']:
            return self.__convert_to_numeric(val, dtype)
        return self.__quote_string(self.__clean_str(val))
    #endregion private methods
    