from typing import Dict, List, Any
from models.FileConfig import FileConfig
from models.DatabaseConfig import DatabaseConfig


class CsvImportConfig:
    database_config:DatabaseConfig
    csv_dir:str
    files:List[FileConfig]
    
    def __init__(
        self
        , csv_dir:str=None
        , files:List[FileConfig]=None) -> None:
        self.csv_dir = csv_dir
        self.files = files
        
    @staticmethod
    def from_dict(csv_import_config:Dict) -> Any:
        csv_dir:str = csv_import_config['csv_dir']
        files:List[FileConfig] = [FileConfig(**file) for file in csv_import_config['files']]
        return CsvImportConfig(csv_dir, files)