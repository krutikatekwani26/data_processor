import pandas as pd

class ExceptionHandler:
    """
    A class responsible for handling all exceptions during the operation execution.
    """

    def handle(self, operation, error: Exception):

        operation_name = type(operation).__name__
        
        if isinstance(error, KeyError):
            print(f"KeyError in '{operation_name}': Column not found in the DataFrame.")
        elif isinstance(error, TypeError):
            print(f"TypeError in '{operation_name}': Incompatible types for the operation.")
        elif isinstance(error, ValueError):
            print(f"ValueError in '{operation_name}': Invalid values found.")
        elif isinstance(error, IndexError):
            print(f"IndexError in '{operation_name}': Index out of bounds.")
        elif isinstance(error, AttributeError):
            print(f"AttributeError in '{operation_name}': Invalid attribute or method for this object.")
        elif isinstance(error, pd.errors.EmptyDataError):
            print(f"EmptyDataError in '{operation_name}': No data found in the DataFrame.")
        elif isinstance(error, pd.errors.ParserError):
            print(f"ParserError in '{operation_name}': Error while parsing the DataFrame.")
        elif isinstance(error, pd.errors.IndexingError):
            print(f"IndexingError in '{operation_name}': Invalid indexing operation on DataFrame.")
        elif isinstance(error, pd.errors.MergeError):
            print(f"MergeError in '{operation_name}': Invalid merge operation attempted on DataFrame.")
        elif isinstance(error, pd.errors.ParserWarning):
            print(f"ParserWarning in '{operation_name}': Issue while parsing the file.")
        elif isinstance(error, FileNotFoundError):
            print(f"FileNotFoundError: The specified file was not found.")
        elif isinstance(error, PermissionError):
            print(f"PermissionError: You do not have permission to access the file or resource.")
        elif isinstance(error, IOError):
            print(f"IOError: There was an issue reading or writing the file.")
        elif isinstance(error, MemoryError):
            print(f"MemoryError: Not enough memory to complete the operation.")
        elif isinstance(error, ZeroDivisionError):
            print(f"ZeroDivisionError: Division by zero occurred.")
        elif isinstance(error, OverflowError):
            print(f"OverflowError: Arithmetic operation resulted in an overflow.")
        elif isinstance(error, RecursionError):
            print(f"RecursionError: Maximum recursion depth exceeded during the operation.")
        elif isinstance(error, NotImplementedError):
            print(f"NotImplementedError: This operation is not yet implemented.")
        elif isinstance(error, ModuleNotFoundError):
            print(f"ModuleNotFoundError: The required module could not be found.")
        elif isinstance(error, ImportError):
            print(f"ImportError: Failed to import a required module.")
        else:
            print(f"Unexpected error in '{operation_name}': {str(error)}")

        raise error
