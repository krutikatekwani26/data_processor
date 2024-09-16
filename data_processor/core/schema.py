import yaml
import pandas as pd
from ..utils.helpers import drop_invalid_columns, validate_column_values

class Schema:
    def __init__(self, schema_path: str):
        with open(schema_path, 'r') as file:
            self.schema = yaml.safe_load(file)

    def drop_invalid_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drop invalid columns from the DataFrame that are not present in the schema.
        """
        print("i am in drop invalid columns")
        return drop_invalid_columns(df, self.schema)

    def validate_column_values(self, df: pd.DataFrame):
        """
        Validate the column values in the DataFrame based on the schema constraints.
        """
        validate_column_values(df, self.schema)

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        First, drop invalid columns, and then validate the column values.
        """
        print("I am here in schema.validatedataframe")
        df = self.drop_invalid_columns(df)  # Drop invalid columns
        self.validate_column_values(df)     # Validate column values
        return df
