#IMPORTS
import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor
from data_processor.processors.merge_processor import MergeProcessor
from data_processor.utils.helpers import *
import warnings
from .core.execution_manager import ExecutionManager
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Load dataset and schema
dataset_path = 'data_processor\input_files\supervision_data_2.xlsx'
schema_path = 'data_processor\input_files\schema.yaml'
dataset_path2 = 'data_processor\input_files\supervison_data.xlsx' 


data = pd.read_excel(dataset_path)
data2 = pd.read_excel(dataset_path2) 



dataset = Dataset(data,schema_path)
dataset2 = Dataset(data2, schema_path)

print(f"before operations{dataset.get_data().shape}")
print(f"before operations{dataset2.get_data().shape}")

# Instantiate processors
cleaning_processor = DataCleaningProcessor()
validation_processor = DataValidationProcessor()
merge_processor = MergeProcessor()
execution_manager = ExecutionManager()

# Add operations to the ExecutionManager
execution_manager.add_operation(2, validation_processor, drop_invalid_columns, [dataset,dataset2])
execution_manager.add_operation(1, cleaning_processor, apply_standard_cleaning,[dataset,dataset2])
execution_manager.add_operation(3, validation_processor, validate_column_values, [dataset,dataset2])
execution_manager.add_operation(4, cleaning_processor, remove_duplicates, [dataset,dataset2])
execution_manager.add_operation(5, merge_processor, add_new_rows, [dataset, dataset2])

# Execute all operations in order
execution_manager.execute()

print(dataset.get_data().shape)
print(dataset2.get_data().shape)


