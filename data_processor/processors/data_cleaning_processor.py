from ..core.dataset import Dataset
from ..utils.helpers import get_operation_list
from ..utils.exception_handler import ExceptionHandler

class DataCleaningProcessor():
    
    def __init__(self):
        
        self.exception_handler = ExceptionHandler() 

    def process_operation(self, operation, dataset: Dataset):
        """
        Process a single cleaning operation on a dataset.
        """
        try:
            df = dataset.get_data()
            df = operation(df)
            dataset.set_data(df)
            
        except Exception as error:
            self.exception_handler.handle(operation, error)
        
        return dataset

    def get_operation_list(self):
        return get_operation_list('cleaning')
