from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..utils.helpers import operation_type_check, SchemaNotProvidedError, get_operation_list
from ..operations.formatting.apply_function import ApplyFunction
from ..utils.exception_handler import ExceptionHandler  # Import ExceptionHandler

class DataValidationProcessor(BaseProcessor):

    def __init__(self):
        super().__init__()
        self.exception_handler = ExceptionHandler()  # Initialize the ExceptionHandler

    @operation_type_check('validation')
    def add_operation(self, func):
        wrapped_operation = ApplyFunction(func)
        super().add_operation(wrapped_operation)
    
    def add_custom_operation(self, operation):
        return super().add_custom_operation(operation)

    def process_operation(self, operation, dataset: Dataset):
        """
        Process a single validation operation on a dataset with the schema.
        """
        try:
            if dataset.get_schema() is None:
                raise SchemaNotProvidedError("Schema required to validate the data.")
            
            if isinstance(operation, ApplyFunction):
                dataset = operation.apply(dataset)
            else:
                df = dataset.get_data()
                schema = dataset.get_schema()
                df = operation(df, schema)
                dataset.set_data(df)
        except Exception as error:
            self.exception_handler.handle(operation, error)

        return dataset

    def get_operation_list(self):
        return get_operation_list('validation')
