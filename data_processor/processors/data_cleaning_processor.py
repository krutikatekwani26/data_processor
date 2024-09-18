from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..utils.helpers import operation_type_check,CleaningOperation


class DataCleaningProcessor(BaseProcessor):

    @operation_type_check(CleaningOperation)
    def add_operation(self, operation):
        super().add_operation(operation)

    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all operations sequentially.
        """
        for operation in self.operations:
            dataset = operation(dataset)
        return dataset
