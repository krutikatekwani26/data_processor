from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..utils.helpers import operation_type_check, get_operation_list
from ..operations.formatting.apply_function import ApplyFunction
from ..utils.exception_handler import ExceptionHandler

class DataCleaningProcessor(BaseProcessor):
    
    def __init__(self):
        super().__init__()
        self.exception_handler = ExceptionHandler() 

    @operation_type_check('cleaning')
    def add_operation(self, func):
        """
        Add an operation to the cleaning processor, with support for additional arguments.
        """
        wrapped_operation = ApplyFunction(func)
        super().add_operation(wrapped_operation)

    def add_custom_operation(self, operation):
        return super().add_custom_operation(operation)

    def process_operation(self, operation, dataset: Dataset):
        """
        Process a single cleaning operation on a dataset.
        """
        try:
            if isinstance(operation, ApplyFunction):
                dataset = operation.apply(dataset)
            else:
                # If not an ApplyFunction, process DataFrame directly
                df = dataset.get_data()
                df = operation(df)
                dataset.set_data(df)
        except Exception as error:
            self.exception_handler.handle(operation, error)
        
        return dataset

    def get_operation_list(self):
        return get_operation_list('cleaning')
