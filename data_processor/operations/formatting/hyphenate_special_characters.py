from ...core.operation import Operation
from ...core.dataset import Dataset
import pandas as pd
from ...utils.helpers import manage_special_characters

class HyphenateSpecialCharacters(Operation):
    """
    Operation to replace spaces around special characters (commas, semicolons, and colons) with hyphens in string columns.
    """
    
    def apply(self, dataset: Dataset) -> Dataset:
        # Get the current data from the dataset
        data = dataset.get_data()

        # Replace spaces around special characters with hyphens
        data = manage_special_characters(data)

        # Set the modified data back to the dataset
        dataset.set_data(data)
        return dataset
