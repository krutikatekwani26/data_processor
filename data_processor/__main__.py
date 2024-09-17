import pandas as pd
from data_processor.core.dataset import Dataset
from data_processor.processors.processor_a import ProcessorA
from data_processor.utils.helpers import *

# Load dataset and schema
dataset_path = 'data_processor\supervison_data.xlsx'
schema_path = 'data_processor/schema.yaml'

data = pd.read_excel(dataset_path)


# Create Dataset object
dataset = Dataset(data)

# Create ProcessorA object and add operations (no need for ApplyFunction, BaseProcessor does that)
processor = ProcessorA()
processor.add_operation(remove_spaces_around_punctuation)
processor.add_operation(make_uppercase)
#processor.add_operation(trim_white_spaces)
processor.add_operation(clean_numeric_values)

# Process dataset
result = processor.process(dataset)


# Save the processed and validated data
print(result.get_data().head())
result.get_data().to_csv('data_processor/check_data.csv', index=False)
