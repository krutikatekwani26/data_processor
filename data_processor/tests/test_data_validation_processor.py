import pytest
import pandas as pd
from processors.data_validation_processor import DataValidationProcessor
from core.dataset import Dataset
from utils.helper import SchemaNotProvidedError


# Example operation to check if a column exists in the DataFrame
def validate_column_exists(df, schema):
    if 'required_column' not in df.columns:
        raise KeyError("required_column is missing")
    return df

# Example operation that validates a column value based on the schema
def validate_column_values(df, schema):
    column = 'existing_column'
    if column in df.columns:
        # Validate if all values are integers
        if not all(isinstance(val, int) for val in df[column]):
            raise ValueError(f"Values in {column} are not integers as expected")
    return df

# Example operation that raises an error (to test exception handling)
def invalid_validation_operation(df, schema):
    raise ValueError("Invalid validation operation for testing")



class TestDataValidationProcessor:

    @pytest.fixture
    def dataset(self, tmp_path):
        # sample DataFrame 
        df = pd.DataFrame({'existing_column': [1, 2, 3]})
        
        #sample schema 
        schema_content = """
        columns:
          existing_column:
            type: integer
        """
        schema_path = tmp_path / "schema.yaml"
        schema_path.write_text(schema_content)

        
        return Dataset(df, schema_path=schema_path)

    @pytest.fixture
    def dataset_without_schema(self):
        
        df = pd.DataFrame({'existing_column': [1, 2, 3]})
        return Dataset(df)

    @pytest.fixture
    def processor(self):
        # Initialize 
        return DataValidationProcessor()

    def test_add_operation(self, processor):
        # Test if operations can be added
        processor.add_custom_operation(validate_column_exists)
        assert len(processor.operations) == 1

    def test_add_multiple_operations(self, processor):
        # Test adding multiple operations
        processor.add_custom_operation(validate_column_exists)
        processor.add_custom_operation(validate_column_values)
        assert len(processor.operations) == 2

    def test_process_operations(self, processor, dataset):
        # Test if operations are processed correctly with a valid schema
        processor.add_custom_operation(validate_column_exists)
        processor.add_custom_operation(validate_column_values)

        
        processed_dataset = processor.process(dataset)
        processed_df = processed_dataset.get_data()

        
        assert 'existing_column' in processed_df.columns

    def test_process_without_schema(self, processor, dataset_without_schema):
        # Test that SchemaNotProvidedError is raised when no schema is provided
        processor.add_custom_operation(validate_column_exists)
        
        with pytest.raises(SchemaNotProvidedError):
            processor.process(dataset_without_schema)

    def test_invalid_operation(self, processor, dataset, capsys):
        # Test handling an invalid operation that raises an error
        processor.add_custom_operation(invalid_validation_operation)

        
        processor.process(dataset)

       
        captured = capsys.readouterr()
        assert "ValueError in 'invalid_validation_operation': Invalid values found.\n" in captured.out

    def test_empty_operations(self, processor, dataset):
        # Test with no operations added
        processed_dataset = processor.process(dataset)
        processed_df = processed_dataset.get_data()

        # The DataFrame should remain unchanged
        assert 'existing_column' in processed_df.columns




    
