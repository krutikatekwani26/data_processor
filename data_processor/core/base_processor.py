# core/base_processor.py
from abc import ABC, abstractmethod
from .dataset import Dataset


class BaseProcessor(ABC):
    @abstractmethod
    def process(self, dataset: Dataset) -> Dataset:
        pass

    @abstractmethod
    def add_operation(self, operation):
        pass
