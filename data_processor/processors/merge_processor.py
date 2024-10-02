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

    def process(self, *datasets: Dataset) -> Dataset:
        """
        Process multiple datasets by merging them sequentially.
        :param datasets: List of Dataset objects to be merged.
        :return: The final merged Dataset.
        """
        try:
            if len(datasets) < 2:
                raise ValueError("At least two datasets are required for merging.")
            
            # Take the first dataset as the base
            dataset1 = datasets[0]
            
            for dataset2 in datasets[1:]:
                df1 = dataset1.get_data()
                df2 = dataset2.get_data()

                # Apply each operation in the operations list
                for operation in self.operations:
                    try:
                        if isinstance(operation, ApplyMerge):
                            dataset1 = operation.apply(dataset1, dataset2)
                        else:
                            df1 = operation(df1, df2)
                            dataset1.set_data(df1)
                    except Exception as error:
                        self.exception_handler.handle(operation, error)

        except Exception as error:
            self.exception_handler.handle(self.process, error)

        return dataset1

    def get_operation_list(self):
        """
        Get the list of available merge operations.
        """
        return get_operation_list('merge')
