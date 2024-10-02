import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor
from data_processor.processors.merge_processor import MergeProcessor
from data_processor.utils.helpers import *

# Load dataset and schema
dataset_path = 'data_processor/supervison_data.xlsx'
schema_path = 'data_processor\schema.yaml'
dataset_path2 = 'data_processor\data2.xlsx'
data = pd.read_excel(dataset_path)
data2 = pd.read_excel(dataset_path)

# Create Dataset object
dataset = Dataset(data, schema_path)
dataset2 = Dataset(data2, schema_path)

#instantiate
cleaning_processor = DataCleaningProcessor()
validation_processor = DataValidationProcessor()
merge_processor = MergeProcessor()


#add operations
cleaning_processor.add_operation(remove_spaces_Around_punctuation)
cleaning_processor.add_operation(manage_special_characters)
cleaning_processor.add_operation(make_uppercase)
cleaning_processor.add_operation(strip_leading_and_trailing_spaces)
cleaning_processor.add_operation(clean_numeric_values)
cleaning_processor.add_operation(remove_duplicates)
validation_processor.add_operation(drop_invalid_columns)
validation_processor.add_operation(validate_column_values)
merge_processor.add_operation(add_new_rows)


#process
cleaned_dataset = cleaning_processor.process(dataset)
validated_dataset = validation_processor.process(dataset)
cleaned_dataset2 = cleaning_processor.process(dataset2)
validated_dataset2 = validation_processor.process(dataset2)
merged_data = merge_processor.process(dataset, dataset2)



print(merged_data.get_data())
#merged_data.get_data().to_csv('data_processor/check_data.csv', index=False)



