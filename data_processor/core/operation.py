# core/operation.py
from abc import ABC, abstractmethod
from .dataset import Dataset


class Operation(ABC):
    @abstractmethod
    def apply(self, dataset: Dataset) -> Dataset|None:
        pass
