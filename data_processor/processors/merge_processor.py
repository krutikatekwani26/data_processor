from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..utils.helpers import operation_type_check, get_operation_list
from ..operations.merging.apply_merge import ApplyMerge  
from ..utils.exception_handler import ExceptionHandler  

class MergeProcessor(BaseProcessor):

    def __init__(self):
        super().__init__()
        self.exception_handler = ExceptionHandler()

    @operation_type_check('merge')
    def add_operation(self, operation, **kwargs):
        wrapped_operation = ApplyMerge(operation, **kwargs)
        super().add_operation(wrapped_operation)

    def add_custom_operation(self, operation):
        if not callable(operation):
            raise ValueError("Custom operation must be a callable function that takes two DataFrames.")
        super().add_operation(operation) 

    def process_operation(self, operation, *dataset_stream: Dataset):
        """
        Process a single merge operation on two datasets.
        """
        try:
            
            
            # Take the first dataset as the base
            main_obj = dataset_stream[0]
            for collection_obj in dataset_stream[1:]:
                
                
                   
                if isinstance(operation, ApplyMerge):
                    main_obj = operation.apply(main_obj, collection_obj)
                else:
                    main_obj_pandas = operation(main_obj.get_data(),collection_obj.get_data())
                    main_obj.set_data(main_obj_pandas)
        

        except Exception as error:
            self.exception_handler.handle(self.process, error)

        return main_obj

    def get_operation_list(self):
        """
        Get the list of available merge operations.
        """
        return get_operation_list('merge')
