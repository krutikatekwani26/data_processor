# apply_merge.py
from data_processor.core.operation import Operation
from ...core.dataset import Dataset

class ApplyMerge(Operation):
    """
    A class to apply merge functions 
    """

    def __init__(self, func, **kwargs):
        self.func = func
        self.kwargs = kwargs  

    def apply(self, dataset1: Dataset, dataset2: Dataset):
        
        df1 = dataset1.get_data()
        df2 = dataset2.get_data()

        # Apply
        result_df = self.func(df1, df2, **self.kwargs)

        dataset1.set_data(result_df)

        return dataset1

