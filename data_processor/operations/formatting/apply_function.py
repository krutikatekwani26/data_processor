from data_processor.core.operation import Operation
from ...core.dataset import Dataset
from ...utils.helpers import requires_schema


class ApplyFunction(Operation):
    """
    Generic operation to apply a specified function to the dataset.
    This class inherits from the abstract Operation class.
    """

    def __init__(self, func):
        self.func = func

    def apply(self, dataset: Dataset):
        data = dataset.get_data()
        schema = dataset.get_schema()

        # Check if the function has the `requires_schema` attribute
        if hasattr(self.func, 'requires_schema') and self.func.requires_schema:
            # If the function requires schema, pass both data and schema
            data = self.func(data, schema)
        else:
            # If the function does not require schema, pass only data
            data = self.func(data)
            
        dataset.set_data(data)
        return dataset
