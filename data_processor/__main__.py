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
dataset_path = 'data_processor/input_excel_files/trial_main_sheet.xlsm'
schema_path = 'data_processor/union_schema.yaml'
dataset_path2 = 'data_processor/input_excel_files/trial_collection_sheet.xlsm'  
data = pd.read_excel(dataset_path)
data2 = pd.read_excel(dataset_path2)

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


# Process all
original_row_counts = (
    dataset.get_data().shape[0], 
    dataset2.get_data().shape[0], 
    dataset3.get_data().shape[0]
)
cleaning_processor.process(dataset, dataset2,dataset3)
validation_processor.process(dataset)
validation_processor.process(dataset2)
validation_processor.process(dataset3)
# Check if the row counts are the same after just cleaning and validating the data
assert (
    dataset.get_data().shape[0], 
    dataset2.get_data().shape[0], 
    dataset3.get_data().shape[0]
) == original_row_counts, "Row count mismatch in one or more datasets after cleaning processing!"



cleaning_processor2.process(dataset,dataset2,dataset3)
#case1: two excat same rows in the main dataset, only 1 should exist
assert dataset.get_data()['HASH ID'].eq('d09cefaaa1c159ccc8b277c17faca50e').sum() == 1, "More than one occurrence of 'd09cefaaa1c159ccc8b277c17faca50e' found!"
#case2: two rows with same hash value but different base and fringe, the one with higher base and fringe should remain
row = dataset.get_data().loc[dataset.get_data()['HASH ID'] == 'c3398e83387943629703b1647d1b9dfa']
assert not row.empty, "Row with 'HASH ID' = 'c3398e83387943629703b1647d1b9dfa' not found!"
assert (row['BASE'].values[0], row['FRINGE'].values[0]) == (39, 29), "Base or fringe value for 'HASH ID' = 'c3398e83387943629703b1647d1b9dfa' is incorrect!"

merge_processor.process(dataset, dataset2)
#case3: override = true confirmed =1 in both dataset
row = dataset.get_data().loc[dataset.get_data()['HASH ID'] == '1f8e0b1fa4eab5c9355d6fd68a1fcd79']
assert not row.empty, "Row with 'HASH ID' = '1f8e0b1fa4eab5c9355d6fd68a1fcd79' not found!"
assert (row['BASE'].values[0], row['FRINGE'].values[0]) == (41, 29), "Base or fringe value for 'HASH ID' = '1f8e0b1fa4eab5c9355d6fd68a1fcd79' is incorrect!"

#case4: override = true confirmed =0 for collection, confirmed =1 for main, the one with confirmed = 0 will remain
row = dataset.get_data().loc[dataset.get_data()['HASH ID'] == '5f04e57e75ce6ea8b89d81e47b5f1301']
assert not row.empty, "Row with 'HASH ID' = '5f04e57e75ce6ea8b89d81e47b5f1301' not found!"
assert (row['BASE'].values[0], row['FRINGE'].values[0]) == (59, 35), "Base or fringe value for 'HASH ID' = '5f04e57e75ce6ea8b89d81e47b5f1301' is incorrect!"

#case5: override = true, but the base fringe are lower in collection than in main. 
row = dataset.get_data().loc[dataset.get_data()['HASH ID'] == '66045c3bb76348cc9e8ebf0999b1ecf4']
assert not row.empty, "Row with 'HASH ID' = '66045c3bb76348cc9e8ebf0999b1ecf4' not found!"
assert (row['BASE'].values[0], row['FRINGE'].values[0]) == (1, 2), "Base or fringe value for 'HASH ID' = '66045c3bb76348cc9e8ebf0999b1ecf4' is incorrect!"

#case6: filter same row working: a row which is the same round up values in main, should not be seen again in main.
row = dataset.get_data().loc[dataset.get_data()['HASH ID'] == 'c64c3d0382a92f4160ababfadd275020']
assert not row.empty, "Row with 'HASH ID' = 'c64c3d0382a92f4160ababfadd275020' not found!"
assert (row['BASE'].values[0], row['FRINGE'].values[0]) == (70.999, 32.111), "Base or fringe value for 'HASH ID' = 'c64c3d0382a92f4160ababfadd275020' is incorrect!"


merge_processor2.process(dataset,dataset3)
#case7: normal non override row merged succesfully and present in main
assert '5405bb8dee296e18cfa4b7cf23ccb124' in dataset.get_data()['HASH ID'].values, "HASH ID '5405bb8dee296e18cfa4b7cf23ccb124' not found in the dataset!"

#case8: normal non override row already exist in main, should not be seen twice in main
assert dataset.get_data()['HASH ID'].eq('d09cefaaa1c159ccc8b277c17faca50e').sum() == 1, "More than one occurrence of 'd09cefaaa1c159ccc8b277c17faca50e' found!"

cleaning_processor3.process(dataset)
#case9: row should be repalced with a confirmed row even though base fringe lower than the one in main
row = dataset.get_data().loc[dataset.get_data()['HASH ID'] == '9472286c1b9d3e12cd2c25e17d80be2a']
assert not row.empty, "Row with 'HASH ID' = '9472286c1b9d3e12cd2c25e17d80be2a' not found!"
assert (row['BASE'].values[0], row['FRINGE'].values[0]) == (22, 12), "Base or fringe value for 'HASH ID' = '9472286c1b9d3e12cd2c25e17d80be2a' is incorrect!"

#case10: final keep largest working for a non override row
row = dataset.get_data().loc[dataset.get_data()['HASH ID'] == '6f8c28babc768e584804eac4ddd46494']
assert not row.empty, "Row with 'HASH ID' = '6f8c28babc768e584804eac4ddd46494' not found!"
assert (row['BASE'].values[0], row['FRINGE'].values[0]) == (97, 98), "Base or fringe value for 'HASH ID' = '6f8c28babc768e584804eac4ddd46494' is incorrect!"


#final confirmatory tests
assert not dataset.get_data()['HASH ID'].duplicated().any(), "Duplicate hash IDs found!"
assert dataset.get_data().shape[0] == 10, "final row count does not match"

# Drop the 'HASH ID' column
#dataset.set_data(dataset.get_data().drop('HASH ID', axis=1))

dataset.get_data().to_csv("data_processor\output excel files\check_data2.csv", index=False)



