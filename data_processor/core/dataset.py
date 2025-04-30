import yaml
import pandas as pd
from typing import Dict, Optional
from utils import (
read_yaml,
gen_hash
)

class Dataset:

    def __init__(self, data: pd.DataFrame, schema: yaml):
        self.data = data
        self.schema = read_yaml(schema) if schema else None
        self._history = {}
        # Preventing the Bastardization of the Initialization Stage
        self._current_hash = gen_hash(data)
        self._update_history(data)


    def _update_history(self, data: pd.DataFrame):
        """Given a pandas object, the method uses pd utility
        to generate a hash, collect useful metadata about the stage
        and logs it to _history class variable.
        :param data: pandas dataframe
        :return: None
        """
        hash_value = gen_hash(data)

        meta_data = {
            'shape': data.shape,
            'columns': list(data.columns),
            'dtypes': data.dtypes.to_dict(),
            'operation': 'init' if not self._history else 'update'
        }

        self._history[hash_value] = {
            'data': data,
            'metadata': meta_data
        }

        # we want the get method to always
        # default to returning the current hash
        self._current_hash = hash_value


    def get_data(self):
        """The data is stored as a dictionary
        so we return the most recent amendment.
        :return: pandas dataset
        """
        return self._history[self._current_hash]['data']

    def set_data(self, update: pd.DataFrame):
        """
        :param update:
        :return:
        """
        self._update_history(update)

    def get_schema(self):
        return self.schema

    def get_current_metadata(self) -> Dict:
        return self._history[self._current_hash]['metadata']

    def get_lineage(self) -> Dict:
        return {hash_: entry['metadata'] for hash_, entry in self._history.items()}

    def get_hash_history(self) -> list:
        return list(self._history.keys())

    def get_data_by_hash(self, hash_value: str) -> Optional[pd.DataFrame]:
        """Incase you messed up or need to validate operation
        Future: I am expecting this to be used as rollback incase
        something is not resolved using the intended operation.

        :param hash_value: it's hopeless if you do not understand what this means : )
        :return: None
        """
        return self._history.get(hash_value, {}).get('data')




