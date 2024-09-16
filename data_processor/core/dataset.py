import pandas as pd
from .schema import Schema  # Import the Schema class

class Dataset:
    def __init__(self, data: pd.DataFrame, schema: Schema = None):
        """
        Initialize the Dataset with a pandas DataFrame and an optional Schema object.
        
        :param data: The pandas DataFrame that contains the data.
        :param schema: Optional schema object for validation.
        """
        self.data = data
        self.schema = schema

    def get_data(self) -> pd.DataFrame:
        """
        Get the current DataFrame stored in the dataset.
        :return: pandas DataFrame
        """
        return self.data

    def set_data(self, data: pd.DataFrame):
        """
        Set a new DataFrame in the dataset.
        :param data: pandas DataFrame
        """
        self.data = data

    def validate(self):
        """
        Validate the dataset against the schema, if a schema is provided.
        This includes:
        1. Dropping columns that are not present in the schema.
        2. Validating column values based on the schema's valid values.
        
        If no schema is provided, print a message.
        """
        print("I am here in dataset.validate")
        if self.schema:
            # Use the schema to validate the DataFrame
            self.data = self.schema.validate_dataframe(self.data)  # Validate the data and update the dataset
        else:
            print("No schema provided for validation.")
