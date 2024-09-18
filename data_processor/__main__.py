import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.data_cleaning_processor import DataCleaningProcessor
from data_processor.processors.data_validation_processor import DataValidationProcessor
from data_processor.utils.helpers import *

# Load dataset and schema
dataset_path = 'data_processor/supervison_data.xlsx'
schema_path = 'data_processor\schema.yaml'
data = pd.read_excel(dataset_path)




# Create Dataset object
dataset = Dataset(data)


cleaning_processor = DataCleaningProcessor()
validation_processor = DataValidationProcessor()

cleaning_processor.add_operation(MakeUppercase())
cleaning_processor.add_operation(RemoveSpacesAroundPunctuation())
cleaning_processor.add_operation(ManageSpecialCharacters())
cleaning_processor.add_operation(StripLeadingAndTrailingSpaces())
cleaning_processor.add_operation(CleanNumericValues())
cleaning_processor.add_operation(RemoveDuplicates())

validation_processor.add_operation(DropInvalidColumns())
validation_processor.add_operation(ValidateColumnValues())




# Process dataset
cleaned_dataset = cleaning_processor.process(dataset)
validated_dataset = validation_processor.process(dataset)



# Save the processed and validated data
print(validated_dataset.get_data().head())
#result.get_data().to_csv('data_processor/check_data.csv', index=False)
