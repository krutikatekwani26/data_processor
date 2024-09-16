from ...core.operation import Operation
from ...core.dataset import Dataset
import pandas as pd
from ...utils.helpers import make_uppercase

class StringUppercase(Operation):
    """
    Operation to convert all string columns and column names to uppercase.
    """

    def apply(self, dataset: Dataset) -> Dataset:
        # Get the current data from the dataset
        data = dataset.get_data()

        # Convert column names and string column values to uppercase
        data = make_uppercase(data)

        # Set the modified data back to the dataset
        dataset.set_data(data)

        return dataset