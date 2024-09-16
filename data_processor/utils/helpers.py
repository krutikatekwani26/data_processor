import pandas as pd

def make_uppercase(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert string columns and column names to uppercase.

    :param df: Input DataFrame whose columns and column names need to be converted to uppercase.
    :return: A new DataFrame with uppercase column names and string column values in uppercase.
    """
    
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


def remove_spaces_around_punctuation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove spaces around specific punctuation marks in string columns of a DataFrame.

    This function iterates through all columns of a DataFrame. If a column's data type is a string,
    it removes spaces around commas, colons, and hyphens within the strings.

    :param df: Input DataFrame whose string columns need spaces removed around specific punctuation marks.
    :return: A new DataFrame with spaces removed around commas, colons, and hyphens in string columns.
    """
    
    # Iterate over each column in the DataFrame
    for col in df.columns:
        # Check if the column's data type is object (string)
        if df[col].dtype == 'object':
            # Remove spaces around commas
            df[col] = df[col].str.replace(r'(\s*,\s*)', ',', regex=True)
            # Remove spaces around colons
            df[col] = df[col].str.replace(r'(\s*:\s*)', ':', regex=True)
            # Remove spaces around hyphens
            df[col] = df[col].str.replace(r'(\s*-\s*)', '-', regex=True)
    
    # Return the modified DataFrame
    return df

def manage_special_characters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace spaces around specific special characters in string columns with hyphens.

    This function iterates through all columns of a DataFrame. If a column's data type is a string,
    it replaces spaces around commas, semicolons, and colons with hyphens.

    :param df: Input DataFrame whose string columns need spaces around special characters replaced with hyphens.
    :return: A new DataFrame with spaces around commas, semicolons, and colons replaced with hyphens in string columns.
    """
    
    # Iterate over each column in the DataFrame
    for col in df.columns:
        # Check if the column's data type is object (string)
        if df[col].dtype == 'object':
            # Replace spaces around commas, semicolons, and colons with hyphens
            df[col] = df[col].str.replace(r'\s*[,;:]\s*', '-', regex=True)
    
    # Return the modified DataFrame
    return df

def strip_leading_and_trailing_spaces(dataframe):
        """
        Strips trailing and leading spaces from column names and categorical string values
        across the entire DataFrame.
        
        Returns:
        pd.DataFrame: The DataFrame with stripped column names and values.
        """
        # Strip spaces from column names
        dataframe.columns = dataframe.columns.str.strip()
        
        # Strip spaces from string data in all categorical/object columns
        for col in dataframe.select_dtypes(include=['object']).columns:
            dataframe[col] = dataframe[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        return dataframe

def clean_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and convert numeric values in a DataFrame.

    This function processes all columns in a DataFrame. It looks for string values
    that start with a dollar sign ('$') and removes the dollar sign before conversion 
    to float. If the conversion fails, it leaves the original value unchanged.

    :param df: The input DataFrame where numeric values need to be cleaned and converted.
    :return: A DataFrame with cleaned numeric values.
    """
    return df.map(lambda x: float(x.replace('$', '')) if isinstance(x, str) and x.startswith('$') 
                       else (float(x) if isinstance(x, str) and x.replace('.', '', 1).isdigit() else x))


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

def validate_column_values(dataframe: pd.DataFrame, schema: dict):
    """
    Validates the values in the DataFrame columns against the valid values specified in the schema.
    If there are invalid values in categorical columns, it raises an error with details.

    :param dataframe: pandas DataFrame that contains the dataset.
    :param schema: dict that contains the schema with valid values for columns.
    :raises ValueError: if any invalid values are found.
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
