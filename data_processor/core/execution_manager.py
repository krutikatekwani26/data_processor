# execution_manager.py
from..processors.merge_processor import MergeProcessor
from ..processors.data_cleaning_processor import DataCleaningProcessor
from ..processors.data_validation_processor import DataValidationProcessor
from ..utils.helpers import check_operation_type

class ExecutionManager:
    def __init__(self):
        # Store all operations with a global order
        self.global_operations = []

    def add_operation(self, order: int, processor, operation, datasets: list):
        
       # Check operation type based on processor type
        if isinstance(processor, DataCleaningProcessor):
            check_operation_type(operation, 'cleaning')
        elif isinstance(processor, DataValidationProcessor):
            check_operation_type(operation, 'validation')
        elif isinstance(processor, MergeProcessor):
            check_operation_type(operation, 'merge')

        self.global_operations.append((order, processor, operation, datasets))

    def add_custom_operation(self, order: int, processor, operation, datasets: list):
        self.global_operations.append((order, processor, operation, datasets))

    def execute(self):
        """
        Execute all operations based on the global order across processors.
        """
        # Sort operations by the specified order
        self.global_operations.sort(key=lambda x: x[0])

        # Loop through each operation in order and execute it
        for _, processor, operation, datasets in self.global_operations:
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
