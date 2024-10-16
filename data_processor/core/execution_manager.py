from processors.merge_processor import MergeProcessor
from processors.data_cleaning_processor import DataCleaningProcessor
from processors.data_validation_processor import DataValidationProcessor
from utils.helper import check_operation_type, validate_order, check_duplicate_order, get_operation_list

class ExecutionManager:
    global_operations = []  # Class-level storage for operations

    def __init__(self):
        pass

    @classmethod
    def add_operation(cls, order: int, processor, operation, datasets: list):
        # Validate order and check for duplicates using helper functions
        validate_order(order)
        check_duplicate_order(order, cls.global_operations)
        
        # Check operation type based on processor type
        if isinstance(processor, DataCleaningProcessor):
            check_operation_type(operation, 'cleaning')
        elif isinstance(processor, DataValidationProcessor):
            check_operation_type(operation, 'validation')
        elif isinstance(processor, MergeProcessor):
            check_operation_type(operation, 'merge')

        # Add the operation to global operations
        cls.global_operations.append((order, processor, operation, datasets))

    @classmethod
    def add_custom_operation(cls, order: int, processor, operation, datasets: list):
        # Validate order and check for duplicates using helper functions
        validate_order(order)
        check_duplicate_order(order, cls.global_operations)

        # Check if the operation is a built-in operation
        built_in_operations = get_operation_list('cleaning') + get_operation_list('validation') + get_operation_list('merge')
        
        if operation.__name__ in built_in_operations:
            raise ValueError(f"The operation '{operation.__name__}' is a built-in operation and cannot be used in add_custom_operation. Use add_operation instead.")
        
        # Add the custom operation to global operations
        cls.global_operations.append((order, processor, operation, datasets))

    @classmethod
    def execute(cls):
        """
        Execute all operations based on the global order across processors.
        """
        # Sort operations by the specified order
        cls.global_operations.sort(key=lambda x: x[0])

        # Loop through each operation in order and execute it
        for _, processor, operation, datasets in cls.global_operations:
            print(f"Executing '{operation.__name__}' on processor '{processor.__class__.__name__}'")
            # Check if processor is MergeProcessor
            if isinstance(processor, MergeProcessor):
                if len(datasets) < 2:
                    raise ValueError("MergeProcessor requires at least two datasets for merging.")
                # For MergeProcessor, pass multiple datasets
                processor.process_operation(operation, *datasets)
            else:
                # For other processors, process each dataset individually
                for dataset in datasets:
                    processor.process_operation(operation, dataset)
