import pandas as pd
from box import ConfigBox
import yaml
from box.exceptions import BoxValueError
from functools import wraps
import inspect
import sys
from typing import List

def mark_as_cleaning_operation(func):
    """
    Decorator to mark a function as a cleaning operation.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    # Mark the function as a cleaning operation
    wrapper._is_cleaning_operation = True
    return wrapper

def mark_as_validation_operation(func):
    """
    Decorator to mark a function as a validation operation.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    # Mark the function as a validation operation
    wrapper._is_validation_operation = True
    return wrapper

def mark_as_merge_operation(func):
    """
    Decorator to mark a function as a merge operation.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    
    # Mark the function as a merge operation
    wrapper._is_merge_operation = True
    return wrapper

class SchemaNotProvidedError(Exception):
    pass

def requires_schema(func):
    """
    Decorator to mark functions that require the schema as an argument.
    """
    func.requires_schema = True
    return func
    



def operation_type_check(operation_type):
    """
    Decorator to ensure the operation being added has been marked with the correct type (cleaning or validation or merge).
    
    :param operation_type: A string representing the type of operation ('cleaning' or 'validation' or 'merge').
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, operation):
            # Build the attribute name dynamically
            expected_attr = f"_is_{operation_type}_operation"
            
            # Check if the operation has the expected attribute
            if not getattr(operation, expected_attr, False):
                raise TypeError(f"Operation must be of type {operation_type}.")
            
            return func(self, operation)
        return wrapper
    return decorator


@mark_as_cleaning_operation
def make_uppercase( dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the uppercase transformation to the DataFrame.
        
        :param dataframe: Input DataFrame whose columns and column names need to be converted to uppercase.
        :param schema: Not used for this operation, included for compatibility.
        :return: A new DataFrame with uppercase column names and string column values in uppercase.
        """

        # Create a copy of the input DataFrame to avoid modifying the original
        _df = dataframe.copy()

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


@mark_as_cleaning_operation
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

@mark_as_cleaning_operation
def remove_spaces_Around_punctuation(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the transformation to remove spaces around commas, colons, and hyphens.
        
        :param dataframe: Input DataFrame whose string columns need spaces removed around specific punctuation marks.
        :param schema: Not used for this operation, included for compatibility.
        :return: A new DataFrame with spaces removed around commas, colons, and hyphens in string columns.
        """

        # Create a copy of the input DataFrame to avoid modifying the original
        _df = dataframe.copy()

        # Iterate over each column in the DataFrame
        for col in _df.columns:
            # Check if the column's data type is object (string)
            if _df[col].dtype == 'object':
                # Remove spaces around commas
                _df[col] = _df[col].str.replace(r'(\s*,\s*)', ',', regex=True)
                # Remove spaces around colons
                _df[col] = _df[col].str.replace(r'(\s*:\s*)', ':', regex=True)
                # Remove spaces around hyphens
                _df[col] = _df[col].str.replace(r'(\s*-\s*)', '-', regex=True)
                _df[col] = _df[col].str.replace(r'(\s*!\s*)', '!', regex=True)
                _df[col] = _df[col].str.replace(r'(\s*;\s*)', ';', regex=True)

        # Return the modified DataFrame
        return _df

@mark_as_validation_operation
@requires_schema
def drop_invalid_columns(dataframe, schema):
        """
        Drops columns from the DataFrame that are not present in the schema.yaml file
        and prints the dropped columns.
        """
        schema_columns = schema['COLUMNS'].keys()
        columns_to_drop = [col for col in dataframe.columns if col not in schema_columns]
        
        if columns_to_drop:
            print(f"Columns dropped: {', '.join(columns_to_drop)}")
        else:
            print("No columns were dropped. All columns are valid.")
    
        return dataframe.drop(columns=columns_to_drop)


@mark_as_cleaning_operation
def manage_special_characters( dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the transformation to replace spaces around commas, semicolons, and colons with hyphens.
        
        :param dataframe: Input DataFrame whose string columns need spaces replaced with hyphens around specific characters.
        :param schema: Not used for this operation, included for compatibility.
        :return: A new DataFrame with spaces around commas, semicolons, and colons replaced with hyphens in string columns.
        """
        
        # Create a copy of the input DataFrame to avoid modifying the original
        _df = dataframe.copy()

        # Iterate over each column in the DataFrame
        for col in _df.columns:
            # Check if the column's data type is object (string)
            if _df[col].dtype == 'object':
                # Replace spaces around commas, semicolons, and colons with hyphens
                _df[col] = _df[col].str.replace(r'\s*[,;:!-]\s*', '-', regex=True)

        # Return the modified DataFrame
        return _df





@mark_as_cleaning_operation
def clean_numeric_values( dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the transformation to clean numeric values in the DataFrame.
        
        :param dataframe: Input DataFrame where numeric values need to be cleaned and converted.
        :param schema: Not used for this operation, included for compatibility.
        :return: A DataFrame with cleaned numeric values.
        """
        
        # Create a copy of the input DataFrame to avoid modifying the original
        _df = dataframe.copy()

        # Clean numeric values by handling dollar signs and converting strings to floats
        _df = _df.map(
            lambda x: float(x.replace('$', '')) if isinstance(x, str) and x.startswith('$')
            else (float(x) if isinstance(x, str) and x.replace('.', '', 1).isdigit() else x)
        )

        return _df

@mark_as_cleaning_operation    
def remove_duplicates(dataframe: pd.DataFrame, subset=None, keep='first') -> pd.DataFrame:
        
        # Identify duplicates
        duplicate_mask = dataframe.duplicated(subset=subset, keep=keep)
        
        # Extract the rows that will be dropped
        dropped_rows = dataframe[duplicate_mask]
        
        
        #if not dropped_rows.empty:
        #    print(f"The following duplicate rows were dropped:\n{dropped_rows}\n")
        
        
        
        return dataframe.drop_duplicates(subset=subset, keep=keep)








@mark_as_validation_operation
@requires_schema
def validate_column_values( dataframe: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """
        Apply the validation to the DataFrame based on the provided schema.
        
        :param dataframe: pandas DataFrame that contains the dataset.
        :param schema: dict that contains the schema with valid values for columns.
        :raises ValueError: if any invalid values are found.
        :return: The unmodified DataFrame if no invalid values are found.
        """

        invalid_data = []

        # Iterate over the schema keys that define valid values
        for column, constraints in schema.items():
            # Check if the column exists in the DataFrame and if the schema has defined valid values for this column
            if column in dataframe.columns and isinstance(constraints, list):
                valid_values = constraints

                # Find invalid values in the DataFrame that are not in the list of valid values
                invalid_values = dataframe[dataframe[column].notna() & ~dataframe[column].isin(valid_values)][column].unique()

                # If invalid values are found, collect the information
                if len(invalid_values) > 0:
                    invalid_data.append(f"Invalid values in column '{column}': {', '.join(map(str, invalid_values))}")

        # Raise ValueError if any invalid values are found
        if invalid_data:
            raise ValueError("\n".join(invalid_data))

        # Return the DataFrame unmodified if no errors
        return dataframe


  
def read_yaml(path_to_yaml: str) -> ConfigBox:
    """
    Read the YAML file and return a ConfigBox object.
    
    This function reads a YAML file from the specified path and converts its content into a ConfigBox object,
    which allows for attribute-style access to dictionary keys. The function ensures that the YAML content 
    is properly formatted as a dictionary and handles various exceptions related to file reading and YAML parsing.
    
    :param path_to_yaml: The file path to the YAML file.
    :return: A ConfigBox object containing the parsed YAML content.
    :raises ValueError: If the YAML content is not a dictionary or if the YAML file is not properly formatted.
    :raises BoxValueError: If the YAML content cannot be converted to a ConfigBox object.
    :raises Exception: For any other unexpected errors.
    """
    try:
        # Open and read the YAML file
        with open(path_to_yaml, 'r') as file:
            content = yaml.safe_load(file)

            # Check if the content is a dictionary
            if isinstance(content, dict):
                
                return ConfigBox(content)
            else:
                raise ValueError("YAML content is not a dictionary")

    except yaml.YAMLError as e:
        # Handle YAML parsing errors
        print(f"An error occurred while parsing the YAML file: {e}")
        raise ValueError("YAML file is not properly formatted")

    except BoxValueError:
        # Handle errors related to converting to ConfigBox
        print("Cannot convert the YAML content to a Box object")
        raise BoxValueError("Cannot convert the YAML content to a Box object")

    except Exception as e:
        # Handle any other unexpected errors
        print(f"An unexpected error occurred: {e}")
        raise e
    
def get_operation_list(operation_type: str) -> List[str]:
    
    attr = {'cleaning': '_is_cleaning_operation', 'validation': '_is_validation_operation','merge': '_is_merge_operation' }.get(operation_type)
    
    if not attr:
        raise ValueError("Invalid operation type. Use 'cleaning' or 'validation' or 'merge'.")
    
    return [name for name, obj in inspect.getmembers(sys.modules[__name__]) 
            if inspect.isfunction(obj) and getattr(obj, attr, False)]
    


def check_no_duplicates(df: pd.DataFrame, subset_columns: list = None) -> bool:
    """
    Check if the DataFrame has any duplicate rows.
    
    :param df: DataFrame to check for duplicates.
    :param subset_columns: List of columns to consider for duplication check (optional).
    :return: True if no duplicates found, raises ValueError otherwise.
    """
    duplicates = df.duplicated(subset=subset_columns)
    
    # If duplicates are found, print them for debugging
    if duplicates.any():
        print("Duplicate rows found:")
        print(df[duplicates])
        raise ValueError(f"DataFrame contains duplicate rows based on columns: {subset_columns if subset_columns else 'all columns'}")
    
    return True

@mark_as_merge_operation
def merge_dfs(df1: pd.DataFrame, df2: pd.DataFrame, merge_columns=None, how="outer") -> pd.DataFrame:
    """
    Merge two DataFrames based on the given columns and merge strategy.

    :param df1: First DataFrame to merge.
    :param df2: Second DataFrame to merge.
    :param merge_columns: List of columns to merge on.
    :param how: Type of merge - 'inner', 'outer', 'left', 'right'.
    :return: Merged DataFrame.
    """
    if merge_columns is None:
        raise ValueError("merge_columns must be provided")

    # Perform the merge using the specified columns and strategy
    merged_df = pd.merge(df1, df2, on=merge_columns, how=how)

    return merged_df

@mark_as_merge_operation
def add_new_rows(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Add rows from df2 into df1. If row exists in df1, it's ignored.

    :param df1: Main DataFrame.
    :param df2: DataFrame containing new rows.
    :return: DataFrame with new rows added from df2.
    """

    # Check that both DataFrames have the same column names
    if not df1.columns.equals(df2.columns):
        raise ValueError(f"DataFrames have different columns. "
                         f"df1 columns: {df1.columns.tolist()}, df2 columns: {df2.columns.tolist()}")

    df1 = remove_duplicates(df1)
    df2 = remove_duplicates(df2)
    
    combined_df = pd.concat([df1, df2]).drop_duplicates(subset=df1.columns.tolist(), keep='first')
    
    # Reset the index 
    combined_df.reset_index(drop=True, inplace=True)
    
    return combined_df