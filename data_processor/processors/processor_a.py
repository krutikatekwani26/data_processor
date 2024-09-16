# This is a placeholder file
# processors/processor_a.py
from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..core.operation import Operation


class ProcessorA(BaseProcessor):
    def __init__(self):
        self.operations = []

    def process(self, dataset: Dataset) -> Dataset:
        for operation in self.operations:
            dataset = operation.apply(dataset)
        return dataset

    def add_operation(self, operation: Operation):
        self.operations.append(operation)
