# execution_manager.py
from..processors.merge_processor import MergeProcessor
class ExecutionManager:
    def __init__(self):
        # Store all operations with a global order
        self.global_operations = []

    def add_operation(self, order: int, processor, operation, datasets: list):
        """
        Add an operation from a processor with its datasets and global execution order.
        """
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
