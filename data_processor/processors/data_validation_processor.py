from ..core.dataset import Dataset
from ..utils.helpers import SchemaNotProvidedError, get_operation_list
from ..utils.exception_handler import ExceptionHandler  

class DataValidationProcessor():

    def __init__(self):
        super().__init__()
        self.exception_handler = ExceptionHandler()  

    def process_operation(self, operation, dataset: Dataset):
        """
        Process a single validation operation on a dataset with the schema.
        """
        try:
            if dataset.get_schema() is None:
                raise SchemaNotProvidedError("Schema required to validate the data.")
            
            
            df = dataset.get_data()
            schema = dataset.get_schema()
            df = operation(df, schema)
            dataset.set_data(df)

        except Exception as error:
            self.exception_handler.handle(operation, error)

        return dataset

    def get_operation_list(self):
        return get_operation_list('validation')
