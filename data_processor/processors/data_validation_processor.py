from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..utils.helpers import operation_type_check,SchemaNotProvidedError
from ..operations.formatting.apply_function import ApplyFunction




class DataValidationProcessor(BaseProcessor):

    @operation_type_check('validation')
    def add_operation(self, operation):
        super().add_operation(operation)
    
    
    def add_custom_operation(self, operation):
        return super().add_custom_operation(operation)

    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all validation operations sequentially.
        
        """
        if dataset.get_schema() is None:
            raise SchemaNotProvidedError("Schema required to validate the data against. Please provide a schema.")

        for operation in self.operations:
            if isinstance(operation, ApplyFunction):
                
                dataset = operation.apply(dataset)
            else:
                df = dataset.get_data()
                schema = dataset.get_schema()
                
                df = operation(df,schema)
                dataset.set_data(df)

        
        return dataset
    
  
