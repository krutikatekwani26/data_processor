from data_processor.core.operation import Operation
from ...core.dataset import Dataset

class ApplyFunction(Operation):
    """
    Generic operation to apply a specified function to the dataset.
    This class inherits from the abstract Operation class.
    """

    def __init__(self, func):
        self.func = func

    def apply(self, dataset: Dataset) -> Dataset:
        data = dataset.get_data()   # Get the current data from the dataset
        data = self.func(data)      # Apply the function to the data
        dataset.set_data(data)      # Set the modified data back to the dataset
        return dataset
