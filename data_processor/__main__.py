# This is a placeholder file
from data_processor.core.dataset import Dataset
from data_processor.core.schema import Schema
from data_processor.processors.processor_a import ProcessorA
from data_processor.operations.formatting import *
import pandas as pd


dataset_path = 'data_processor\Current Labor Rate Database (Projects).xlsx'
schema_path = 'data_processor\schema.yaml'

# Load dataset
data = pd.read_excel(dataset_path)

# Load schema
schema = Schema(schema_path)

# Create Dataset object (no validation initially)
dataset1 = Dataset(data, schema)



# Create processor
processor = ProcessorA()

# Add operations
processor.add_operation(StringUppercase())
processor.add_operation(CleanPunctuationSpacing())
processor.add_operation(HyphenateSpecialCharacters())
processor.add_operation(TrimWhiteSpaces())
processor.add_operation(CleanNumericValues())


# Process dataset

result = processor.process(dataset1)
result.validate()
result.get_data().to_csv('data_processor\check_data.csv', index=False)


