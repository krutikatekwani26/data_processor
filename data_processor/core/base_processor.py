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
    def add_custom_operation(self, operation):
        """
        Allows the user to add custom operations that do not need to pass through the
        operation type check.
        """
        self.operations.append(operation)

    @abstractmethod
    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all operations sequentially.
        This method must be implemented by subclasses.
        """
        pass
