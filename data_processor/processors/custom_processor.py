from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset


class CustomProcessor(BaseProcessor):

    def add_operation(self, operation):
        """
        Add a custom operation to the processor. 
        The operation can be any function or callable that takes a DataFrame and returns a modified DataFrame.
        """
        # Add the operation directly to the list without any checks or wrappers
        self.operations.append(operation)

    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all custom operations sequentially.
        """
        df = dataset.get_data()
        schema = dataset.get_schema()

        for operation in self.operations:
            try:
                
                df = operation(df, schema)
            except TypeError:
                
                df = operation(df)
        
        dataset.set_data(df)
        return dataset
