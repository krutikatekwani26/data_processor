from ...core.operation import Operation
from ...core.dataset import Dataset
import pandas as pd
from ...utils.helpers import remove_spaces_around_punctuation

class CleanPunctuationSpacing(Operation):
    """
    Operation to clean unnecessary spaces around punctuation marks like commas, periods, hyphens, and colons in string columns.
    """
    
    def apply(self, dataset: Dataset) -> Dataset:
        # Get the current data from the dataset
        data = dataset.get_data()

        # Remove spaces around punctuation marks
        data = remove_spaces_around_punctuation(data)

        # Set the modified data back to the dataset
        dataset.set_data(data)
        return dataset