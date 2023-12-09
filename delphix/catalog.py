
from typing import Protocol, Dict
import pandas as pd

class Catalog(Protocol):
    def resolve_name(self, name: str) -> str:
        ...

    

class LocalCatalog(Catalog):
    def __init__(self, entries: Dict[str, str]):
        self._entries = entries

    def resolve_name(self, name: str) -> str:
        return self._entries[name]



class PandasDataFrameLoader(Protocol):
    def load(self, path: str) -> pd.DataFrame:
        ...



class PandasFileLoader(PandasDataFrameLoader):
    def __init__(self, sep: str = ','):
        self.sep = sep

    def load(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path, sep=self.sep)
    
