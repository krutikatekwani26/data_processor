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
    def add_operation(self, operation):
        super().add_operation(operation)
    
    def add_custom_operation(self, operation):
        return super().add_custom_operation(operation)

    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all validation operations sequentially.
        Apply validation based on a provided schema.
        """

        if dataset.get_schema() is None:
            raise SchemaNotProvidedError("Schema required to validate the data against. Please provide a schema.")
        
        # Apply each validation operation in sequence
        for operation in self.operations:
            try:
                if isinstance(operation, ApplyFunction):
                    # Apply built-in validation operations
                    dataset = operation.apply(dataset)
                else:
                    # Apply custom validation operations
                    df = dataset.get_data()
                    schema = dataset.get_schema()
                    df = operation(df, schema)
                    dataset.set_data(df)
            except Exception as error:
                
                self.exception_handler.handle(operation, error)

        return dataset

    def get_operation_list(self):
        return get_operation_list('validation')
