import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor
from data_processor.processors.merge_processor import MergeProcessor
from data_processor.utils.helpers import *
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Load dataset and schema
dataset_path = 'data_processor/supervison_data.xlsx'
schema_path = 'data_processor/schema.yaml'
dataset_path2 = 'data_processor/data2.xlsx'  
dataset_path3 = 'data_processor/data3.xlsx'
data = pd.read_excel(dataset_path)
data2 = pd.read_excel(dataset_path2)
data3 = pd.read_excel(dataset_path3)

# Create Dataset objects
dataset = Dataset(data, schema_path)
dataset2 = Dataset(data2, schema_path)
dataset3 = Dataset(data3, schema_path)

# Instantiate processors
cleaning_processor = DataCleaningProcessor()
validation_processor = DataValidationProcessor()
merge_processor = MergeProcessor()

print(merge_processor.get_operation_list())

# Add cleaning operations
cleaning_processor.add_operation(remove_spaces_Around_punctuation)
cleaning_processor.add_operation(manage_special_characters)
cleaning_processor.add_operation(make_uppercase)
cleaning_processor.add_operation(strip_leading_and_trailing_spaces)
cleaning_processor.add_operation(clean_numeric_values)
cleaning_processor.add_operation(remove_duplicates)

# Add validation operations
validation_processor.add_operation(drop_invalid_columns)
validation_processor.add_operation(validate_column_values)

# Add merge operation
merge_processor.add_operation(add_new_rows)



# Process cleaning
cleaning_processor.process(dataset)
cleaning_processor.process(dataset2)
cleaning_processor.process(dataset3)
print(dataset.get_data().shape)
print(dataset2.get_data().shape)
print(dataset3.get_data().shape)




# Process validation
validation_processor.process(dataset)
validation_processor.process(dataset2)
validation_processor.process(dataset3)
#validated_dataset.get_data().to_csv("data_processor/midcheck_data.csv", index=False)

# Merge the datasets
merged_data = merge_processor.process(dataset, dataset2,dataset3)

# Print the merged data

print(dataset.get_data().shape)
print(dataset.get_data().head())

dataset.get_data().to_csv("data_processor/check_data.csv", index=False)


