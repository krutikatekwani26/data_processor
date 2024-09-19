import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor
from data_processor.processors.custom_processor import CustomProcessor
from data_processor.utils.helpers import *

# Load dataset and schema
dataset_path = 'data_processor/supervison_data.xlsx'
schema_path = 'data_processor\schema.yaml'
data = pd.read_excel(dataset_path)

def make_uppercase(df: pd.DataFrame) -> pd.DataFrame:
    
    # Create a copy of the input DataFrame to avoid modifying the original
    _df = df.copy()

    # Convert all column names to uppercase
    _df.columns = [col.upper() for col in _df.columns]

    # Iterate over each column in the DataFrame
    for col in _df.columns:
        # Check if the column's data type is object (string) or string
        if _df[col].dtype == 'object' or _df[col].dtype == 'string':
            # Convert all values in the column to uppercase
            _df[col] = _df[col].str.upper()

    # Return the modified DataFrame
    return _df

def drop_invalid_columns(dataframe, schema):
        """
        Drops columns from the DataFrame that are not present in the schema.yaml file
        and prints the dropped columns.
        
        Returns:
        pd.DataFrame: The DataFrame with only valid columns according to the schema.
        """
        schema_columns = schema['COLUMNS'].keys()  # Get valid columns from the schema
        columns_to_drop = [col for col in dataframe.columns if col not in schema_columns]
        
        # Print the columns that are being dropped
        if columns_to_drop:
            print(f"Columns dropped: {', '.join(columns_to_drop)}")
        else:
            print("No columns were dropped. All columns are valid.")

        # Drop columns that are not in the schema
        dataframe = dataframe.drop(columns=columns_to_drop)
        
        return dataframe


# Create Dataset object
dataset = Dataset(data,schema_path)


cleaning_processor = DataCleaningProcessor()
validation_processor = DataValidationProcessor()
custom = CustomProcessor()

custom.add_operation(make_uppercase)
custom.add_operation(drop_invalid_columns)

#cleaning_processor.add_operation(MakeUppercase())
#cleaning_processor.add_operation(RemoveSpacesAroundPunctuation())
#cleaning_processor.add_operation(ManageSpecialCharacters())
#cleaning_processor.add_operation(StripLeadingAndTrailingSpaces())
#cleaning_processor.add_operation(CleanNumericValues())
#cleaning_processor.add_operation(RemoveDuplicates())

#validation_processor.add_operation(DropInvalidColumns())
#validation_processor.add_operation(ValidateColumnValues())




# Process dataset
#cleaned_dataset = cleaning_processor.process(dataset)

custom_dataset = custom.process(dataset)
validated_dataset = validation_processor.process(dataset)



# Save the processed and validated data
print(custom_dataset.get_data().head())
#result.get_data().to_csv('data_processor/check_data.csv', index=False)
