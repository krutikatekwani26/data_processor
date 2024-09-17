import pandas as pd
from ..utils.helpers import read_yaml


class Dataset:
    def __init__(self, data: pd.DataFrame, schema_path = None):
        
        self.data = data
        self.schema = read_yaml(schema_path) if schema_path else None
        

    def get_data(self) -> pd.DataFrame:
       
    
        return self.data

    def set_data(self, data: pd.DataFrame):
        
        self.data = data
