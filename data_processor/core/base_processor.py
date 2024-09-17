from abc import ABC, abstractmethod
from data_processor.core.dataset import Dataset
from ..operations.formatting.apply_function import ApplyFunction

class BaseProcessor(ABC):
    def __init__(self):
        self.operations = []

    def add_operation(self, func):
        """
        Add a function operation to the list of operations.
        Automatically wraps the function into the ApplyFunction class.
        """
        self.operations.append(ApplyFunction(func))

    @abstractmethod
    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all operations sequentially.
        This method must be implemented by subclasses.
        """
        pass
