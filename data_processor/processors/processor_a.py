from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..core.operation import Operation
from data_processor.operations.formatting.apply_function import ApplyFunction

class ProcessorA(BaseProcessor):
    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all operations sequentially.
        """
        for operation in self.operations:
            dataset = operation.apply(dataset)
        return dataset
