from ...core.operation import Operation
from ...core.dataset import Dataset
import pandas as pd
from ...utils.helpers import strip_leading_and_trailing_spaces




class TrimWhiteSpaces(Operation):
    """
    Operation to remove spaces around hyphen, colon, punctuation marks.
    """

    def apply(self, dataset: Dataset) -> Dataset:
        # Get the current data from the dataset
        data = dataset.get_data()

        # Convert column names and string column values to uppercase
        data = strip_leading_and_trailing_spaces(data)

        # Set the modified data back to the dataset
        dataset.set_data(data)

        return dataset