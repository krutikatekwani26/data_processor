from ...core.operation import Operation
from ...core.dataset import Dataset
import pandas as pd
from ...utils.helpers import clean_numeric_values

class CleanNumericValues(Operation):
    """
    Operation to clean and convert numeric values in string columns.
    
    This includes removing dollar signs, converting numeric strings to floats, 
    and leaving non-convertible values unchanged.
    """
    
    def apply(self, dataset: Dataset) -> Dataset:
        # Get the current data from the dataset
        data = dataset.get_data()

        # Replace spaces around special characters with hyphens
        data = clean_numeric_values(data)

        # Set the modified data back to the dataset
        dataset.set_data(data)
        return dataset