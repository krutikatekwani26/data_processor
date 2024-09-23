import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor


from data_processor.utils.helpers import *

# Load dataset and schema
dataset_path = 'data_processor/supervison_data.xlsx'
schema_path = 'data_processor\schema.yaml'
data = pd.read_excel(dataset_path)

def strip_leading_and_trailing_spaces( dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the transformation to strip spaces from column names and string values.
        
        :param dataframe: Input DataFrame to strip spaces from.
        :param schema: Not used for this operation, included for compatibility.
        :return: A DataFrame with stripped column names and values.
        """

        # Create a copy of the input DataFrame to avoid modifying the original
        _df = dataframe.copy()

        # Strip spaces from column names
        _df.columns = _df.columns.str.strip()

        # Strip spaces from string data in all categorical/object columns
        for col in _df.select_dtypes(include=['object']).columns:
            _df[col] = _df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

        return _df






# Create Dataset object
dataset = Dataset(data,schema_path)


cleaning_processor = DataCleaningProcessor()
validation_processor = DataValidationProcessor()
#custom = CustomProcessor()


print(cleaning_processor.get_operation_list())
print(validation_processor.get_operation_list())

cleaning_processor.add_custom_operation(strip_leading_and_trailing_spaces)



cleaning_processor.add_operation(make_uppercase)
cleaning_processor.add_operation(remove_spaces_Around_punctuation)
cleaning_processor.add_operation(manage_special_characters)
cleaning_processor.add_custom_operation(strip_leading_and_trailing_spaces)
cleaning_processor.add_operation(clean_numeric_values)
validation_processor.add_operation(drop_invalid_columns)
validation_processor.add_operation(validate_column_values)


#cleaning_processor.add_operation(RemoveDuplicates())

#validation_processor.add_operation(DropInvalidColumns())
#validation_processor.add_operation(ValidateColumnValues())




# Process dataset
#cleaned_dataset = cleaning_processor.process(dataset)

#custom_dataset = cleaning_processor.process(dataset)
cleaned_dataset = cleaning_processor.process(dataset)
validated_dataset = validation_processor.process(dataset)



# Save the processed and validated data

print(validated_dataset.get_data().head())
#validated_dataset.get_data().to_csv('data_processor/check_data.csv', index=False)



