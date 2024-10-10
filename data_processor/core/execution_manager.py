from ..processors.merge_processor import MergeProcessor
from ..processors.data_cleaning_processor import DataCleaningProcessor
from ..processors.data_validation_processor import DataValidationProcessor
from ..utils.helpers import check_operation_type

class ExecutionManager:
    global_operations = []  # Class-level storage for operations


    def __init__(self):
        pass

    @classmethod
    def add_operation(cls, order: int, processor, operation, datasets: list):
        # Check operation type based on processor type
        if isinstance(processor, DataCleaningProcessor):
            check_operation_type(operation, 'cleaning')
        elif isinstance(processor, DataValidationProcessor):
            check_operation_type(operation, 'validation')
        elif isinstance(processor, MergeProcessor):
            check_operation_type(operation, 'merge')

        cls.global_operations.append((order, processor, operation, datasets))

    @classmethod
    def add_custom_operation(cls, order: int, processor, operation, datasets: list):
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
