import pytest
import pandas as pd
from ..processors.data_cleaning_processor import DataCleaningProcessor
from ..core.dataset import Dataset


# Example operation to add a new column to the DataFrame
def add_column_operation(df):
    df['new_column'] = df['existing_column'] + 1
    return df

# Example operation to rename a column
def rename_column_operation(df):
    return df.rename(columns={'existing_column': 'renamed_column'})

# Example operation that raises an error
def invalid_operation(df):
    raise ValueError("Invalid operation for testing")



class TestDataCleaningProcessor:
    
    @pytest.fixture
    def dataset(self):
       
        df = pd.DataFrame({'existing_column': [1, 2, 3]})
        return Dataset(df)

    @pytest.fixture
    def processor(self):
        
        return DataCleaningProcessor()

    def test_add_operation(self, processor):
        # Test if operations can be added
        processor.add_custom_operation(add_column_operation)
        assert len(processor.operations) == 1

    def test_add_multiple_operations(self, processor):
        # Test adding multiple operations
        processor.add_custom_operation(add_column_operation)
        processor.add_custom_operation(rename_column_operation)
        assert len(processor.operations) == 2

    def test_process_operations(self, processor, dataset):
        # Test if operations are processed correctly
        processor.add_custom_operation(add_column_operation)
        processor.add_custom_operation(rename_column_operation)

        
        processed_dataset = processor.process(dataset)
        processed_df = processed_dataset.get_data()

        # Assert that both operations were applied
        assert 'new_column' in processed_df.columns
        assert 'renamed_column' in processed_df.columns

    def test_empty_operations(self, processor, dataset):
        # Test with no operations added
        processed_dataset = processor.process(dataset)
        processed_df = processed_dataset.get_data()

        assert 'existing_column' in processed_df.columns
        assert 'new_column' not in processed_df.columns

    def test_invalid_operation(self, processor, dataset, capsys):
        # Test handling an invalid operation that raises an error
        processor.add_custom_operation(invalid_operation)
        
        processor.process(dataset)
        captured = capsys.readouterr()
        assert "ValueError in 'invalid_operation': Invalid values found.\n" in captured.out
