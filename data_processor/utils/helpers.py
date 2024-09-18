import pandas as pd
from box import ConfigBox
import yaml
from box.exceptions import BoxValueError
from functools import wraps

class CleaningOperation:
    pass

class ValidationOperation:
   
    pass
    

def operation_type_check(allowed_type):
    """
    Decorator to ensure the operation being added is of the allowed type.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, operation):
            # Now we're using isinstance() to check if the operation is the correct type
            if not isinstance(operation, allowed_type):
                raise TypeError(f"Operation must be of type {allowed_type.__name__}")
            return func(self, operation)
        return wrapper
    return decorator

class MakeUppercase(CleaningOperation):
    """
    Convert string columns and column names to uppercase.
    This class performs the operation on the provided DataFrame.
    """

    def __call__(self, dataframe: pd.DataFrame) -> pd.DataFrame:
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



class RemoveSpacesAroundPunctuation(CleaningOperation):
    """
    Remove spaces around specific punctuation marks in string columns of a DataFrame.
    This class performs the operation on the provided DataFrame.
    """

    def __call__(self, dataframe: pd.DataFrame) -> pd.DataFrame:
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

        # Return the modified DataFrame
        return _df



class ManageSpecialCharacters(CleaningOperation):
    """
    Replace spaces around specific special characters in string columns with hyphens.
    This class performs the operation on the provided DataFrame.
    """

    def __call__(self, dataframe: pd.DataFrame) -> pd.DataFrame:
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
                _df[col] = _df[col].str.replace(r'\s*[,;:]\s*', '-', regex=True)

        # Return the modified DataFrame
        return _df



class StripLeadingAndTrailingSpaces(CleaningOperation):
    """
    Strips trailing and leading spaces from column names and categorical string values across the entire DataFrame.
    """

    def __call__(self, dataframe: pd.DataFrame) -> pd.DataFrame:
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

class CleanNumericValues(CleaningOperation):
    """
    Clean and convert numeric values in a DataFrame.
    This class handles cases where strings contain dollar signs and converts them to float values.
    """

    def __call__(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the transformation to clean numeric values in the DataFrame.
        
        :param dataframe: Input DataFrame where numeric values need to be cleaned and converted.
        :param schema: Not used for this operation, included for compatibility.
        :return: A DataFrame with cleaned numeric values.
        """
        
        # Create a copy of the input DataFrame to avoid modifying the original
        _df = dataframe.copy()

        # Clean numeric values by handling dollar signs and converting strings to floats
        _df = _df.applymap(
            lambda x: float(x.replace('$', '')) if isinstance(x, str) and x.startswith('$')
            else (float(x) if isinstance(x, str) and x.replace('.', '', 1).isdigit() else x)
        )

        return _df
    
class RemoveDuplicates(CleaningOperation):
    """
    A class to remove duplicate rows from a DataFrame.
    
    """

    def __call__(self, dataframe: pd.DataFrame, subset=None, keep='first') -> pd.DataFrame:
        
        # Identify duplicates
        duplicate_mask = dataframe.duplicated(subset=subset, keep=keep)
        
        # Extract the rows that will be dropped
        dropped_rows = dataframe[duplicate_mask]
        
        # Print the rows that are being dropped
        if not dropped_rows.empty:
            print(f"The following duplicate rows were dropped:\n{dropped_rows}\n")
        
        
        # Remove duplicates using Pandas' drop_duplicates function
        return dataframe.drop_duplicates(subset=subset, keep=keep)





class DropInvalidColumns(ValidationOperation):
    def __call__(self, dataframe, schema):
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




class ValidateColumnValues(ValidationOperation):
    """
    Validates the values in the DataFrame columns against the valid values specified in the schema.
    This class performs validation and raises an error if invalid values are found.
    """

    def __call__(self, dataframe: pd.DataFrame, schema: dict) -> pd.DataFrame:
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
    



