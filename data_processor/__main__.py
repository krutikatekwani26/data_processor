#IMPORTS
import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor
from data_processor.processors.merge_processor import MergeProcessor
from data_processor.utils.helpers import *
import warnings
from .user_custom_methods import *
from .core.execution_manager import ExecutionManager
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Load dataset and schema
dataset_path = 'data_processor/input_files/raw_main_df.csv'
schema_path = 'data_processor/input_files/union_schema.yaml'
dataset_path2 = 'data_processor/input_files/raw_rh_df.csv'  
dataset_path3 = 'data_processor/input_files/raw_rp_df.csv'
data = pd.read_csv(dataset_path)
data2 = pd.read_csv(dataset_path2)
data3 = pd.read_csv(dataset_path3)

if "OVERRIDE" in data2.columns:
    
    # Split the DataFrame into override and non-override parts
    override_df = data2[data2["OVERRIDE"] == True].copy()
    non_override_df = data2[data2["OVERRIDE"] != True].copy()
    # Drop the OVERRIDE column from both DataFrames
    override_df = override_df.drop("OVERRIDE", axis=1)
    non_override_df = non_override_df.drop("OVERRIDE", axis=1)

# Create Dataset objects
dataset = Dataset(data, schema_path)
dataset2 = Dataset(override_df, schema_path)
dataset3 = Dataset(non_override_df, schema_path)
dataset4 = Dataset(data3,schema_path)
all_datasets = [dataset, dataset2, dataset3, dataset4]

# Instantiate processors
cleaning_processor = DataCleaningProcessor()
validation_processor = DataValidationProcessor()
merge_processor = MergeProcessor()
execution_manager = ExecutionManager()

# Add operations to the ExecutionManager
execution_manager.add_operation(1, cleaning_processor, apply_standard_cleaning, all_datasets)
execution_manager.add_custom_operation(2, cleaning_processor, convert_journeyman_to_journey, all_datasets)
execution_manager.add_custom_operation(3, validation_processor, check_city, all_datasets)
execution_manager.add_operation(4, validation_processor, drop_invalid_columns, all_datasets)
execution_manager.add_operation(5, validation_processor, validate_column_values, all_datasets)
execution_manager.add_custom_operation(6, cleaning_processor, generate_hash, all_datasets)
execution_manager.add_custom_operation(7, cleaning_processor, keep_the_largest_dup, all_datasets)
execution_manager.add_custom_operation(8, merge_processor, handle_override, [dataset, dataset2])
execution_manager.add_operation(9, merge_processor, add_new_rows, [dataset, dataset3,dataset4])
execution_manager.add_custom_operation(10, cleaning_processor, replace_unconfirmed, [dataset])
execution_manager.add_custom_operation(11, cleaning_processor, keep_the_largest_dup, [dataset])

# Execute all operations in order
execution_manager.execute()

dataset.set_data(dataset.get_data().drop('HASH ID', axis=1))

print(dataset.get_data().shape)
print(dataset.get_data().head())
