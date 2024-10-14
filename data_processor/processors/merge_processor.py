from core.dataset import Dataset
from utils.helper import  get_operation_list
from utils.exception_handler import ExceptionHandler

class MergeProcessor:

    def __init__(self):
        
        self.exception_handler = ExceptionHandler()

    def process_operation(self, operation, *dataset_stream: Dataset):
        main_obj = None
        try:
            # Take the first dataset as the base
            main_obj = dataset_stream[0]
            print(f"Printing shape of main object in merge processor: - {main_obj.get_data().shape}")
            for collection_obj in dataset_stream[1:]:
                main_obj_pandas = operation(main_obj.get_data(),collection_obj.get_data())
                main_obj.set_data(main_obj_pandas)
        
        except Exception as error:
            print(f'Merge Processor failed on {operation} {error}')
            self.exception_handler.handle(operation, error)

        return main_obj

    def get_operation_list(self):
        """
        Get the list of available merge operations.
        """
        return get_operation_list('merge')
