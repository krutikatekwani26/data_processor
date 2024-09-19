from ..core.base_processor import BaseProcessor
from ..core.dataset import Dataset
from ..utils.helpers import operation_type_check,ValidationOperation


class SchemaNotProvidedError(Exception):
    pass

class DataValidationProcessor(BaseProcessor):

    @operation_type_check(ValidationOperation)
    def add_operation(self, operation):
        super().add_operation(operation)

    def process(self, dataset: Dataset) -> Dataset:
        """
        Process the dataset by applying all validation operations sequentially.
        
        """
        if dataset.get_schema() is None:
            raise SchemaNotProvidedError("Schema required to validate the data against. Please provide a schema.")

        for operation in self.operations:
            dataset = operation(dataset)
        return dataset
