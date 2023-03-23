
class FileConfig:
    source:str
    table_schema:str
    table_name:str
    include:bool
    truncate:bool
    delimiter:str=';'
    deprecated:bool=False,
    update_primary_key:bool=False
    comment:str
    
    def __init__(
        self
        , source:str=None
        , table_schema:str=None
        , table_name:str=None
        , include:bool=True
        , truncate:bool=False
        , delimiter:str=';'
        , comment:str=None) -> None:
        
        self.source = source
        self.table_schema = table_schema
        self.table_name = table_name
        self.include = include
        self.truncate = truncate
        self.delimiter = delimiter
        self.comment = comment
        
    def to_dict(self) -> dict[str, any]:
        return self.__dict__
    
    @staticmethod
    def from_dict(d:dict):
        return FileConfig(**d)