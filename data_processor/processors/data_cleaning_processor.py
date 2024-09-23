from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..utils.helpers import operation_type_check,get_operation_list
from ..operations.formatting.apply_function import ApplyFunction


class DataCleaningProcessor(BaseProcessor):

    @operation_type_check('cleaning')
    def add_operation(self, operation):
        super().add_operation(operation)

    def add_custom_operation(self, operation):
        return super().add_custom_operation(operation)

    def process(self, dataset: Dataset) -> Dataset:
        

        # Apply each operation in sequence
        for operation in self.operations:
            if isinstance(operation, ApplyFunction):
               
                dataset = operation.apply(dataset)
            else:
                df = dataset.get_data()
                df = operation(df)
                dataset.set_data(df) 

        
        
        return dataset
    
    def get_operation_list(self):
        
        return get_operation_list('cleaning')
    
    
    