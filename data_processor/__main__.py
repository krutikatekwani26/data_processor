#IMPORTS
import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor
from data_processor.processors.merge_processor import MergeProcessor
from data_processor.utils.helpers import *
import warnings
from .user_custom_methods import *
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Load dataset and schema
dataset_path = 'data_processor/input_excel_files/raw_main_df.csv'
schema_path = 'data_processor/union_schema.yaml'
dataset_path2 = 'data_processor/input_excel_files/raw_rh_df.csv'  
dataset_path3 = 'data_processor/input_excel_files/raw_rp_df.csv'
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

# Instantiate processors
cleaning_processor = DataCleaningProcessor()
cleaning_processor2 = DataCleaningProcessor()
cleaning_processor3 = DataCleaningProcessor()
validation_processor = DataValidationProcessor()
merge_processor = MergeProcessor()
merge_processor2 = MergeProcessor()


# Add all operations
cleaning_processor.add_operation(apply_standard_cleaning)
cleaning_processor.add_custom_operation(convert_journeyman_to_journey)

validation_processor.add_custom_operation(check_city)
validation_processor.add_operation(drop_invalid_columns)
validation_processor.add_operation(validate_column_values)


cleaning_processor2.add_custom_operation(generate_hash)
cleaning_processor2.add_custom_operation(keep_the_largest_dup)

merge_processor.add_custom_operation(handle_override)
merge_processor2.add_operation(add_new_rows)

cleaning_processor3.add_custom_operation(replace_unconfirmed)
cleaning_processor3.add_custom_operation(keep_the_largest_dup)



cleaning_processor.process(dataset, dataset2,dataset3,dataset4)
validation_processor.process(dataset)
validation_processor.process(dataset2)
validation_processor.process(dataset3)
validation_processor.process(dataset4)
print(f"after initial cleaning and validation {dataset.get_data().shape[0]}")
print(dataset2.get_data().shape[0])
print(dataset3.get_data().shape[0])
print(dataset4.get_data().shape[0])

cleaning_processor2.process(dataset,dataset2,dataset3,dataset4)
dataset2.get_data().to_csv("data_processor\output excel files\checking_override_hash.csv", index=False)
print(f"after initial keep largest duplicate {dataset.get_data().shape[0]}")
print(dataset2.get_data().shape[0])
print(dataset3.get_data().shape[0])
print(dataset4.get_data().shape[0])


merge_processor.process(dataset, dataset2)
merge_processor2.process(dataset,dataset3,dataset4)

cleaning_processor3.process(dataset)


#final confirmatory tests
assert not dataset.get_data()['HASH ID'].duplicated().any(), "Duplicate hash IDs found!"

# Drop the 'HASH ID' column
#dataset.set_data(dataset.get_data().drop('HASH ID', axis=1))
print(f"final count for main {dataset.get_data().shape[0]}")

dataset.get_data().to_csv("data_processor\output excel files\check_data2.csv", index=False)



