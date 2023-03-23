from typing import Dict, Any

class DatabaseConfig:
    server:str = 'localhost'
    dbname:str = ''
    is_trusted:str = 'no'
    
    def __init__(self, server:str='localhost', dbname:str='', is_trusted:str='no') -> None:
        self.server = server
        self.dbname = dbname
        self.is_trusted = is_trusted
        
    @staticmethod
    def from_dict(database_config:Dict) -> Any:
        return DatabaseConfig(**database_config)