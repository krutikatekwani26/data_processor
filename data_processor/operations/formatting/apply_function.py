from data_processor.core.operation import Operation
from ...core.dataset import Dataset

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

        try:
            data = self.func(data, schema)
        except TypeError:
            data = self.func(data)  
            
        dataset.set_data(data)      
        return dataset
    
    def __call__(self, dataset):
        return self.apply(dataset)
