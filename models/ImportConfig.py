import os
from typing import Dict, List, Any
import json
from models.FileConfig import FileConfig
from models.DatabaseConfig import DatabaseConfig
from models.CsvImportConfig import CsvImportConfig

class ImportConfig:
    database_config:DatabaseConfig
    csv_import_config:CsvImportConfig
    
    def __init__(self, database_config:DatabaseConfig=None, csv_import_config:CsvImportConfig=None) -> None:
        self.database_config = database_config
        self.csv_import_config = csv_import_config
        
    @staticmethod
    def from_json(filepath:str) -> Any:
        data:Dict = {}
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"{filepath} does not exist.")
        with open(filepath, 'r', encoding="utf-8") as f:
            data = json.loads(f.read())
        database_config:DatabaseConfig = DatabaseConfig.from_dict(data['database_config'])
        csv_import_config:CsvImportConfig = CsvImportConfig.from_dict(data['csv_import_config'])
        return ImportConfig(database_config, csv_import_config)