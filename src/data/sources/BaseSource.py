from abc import ABC, abstractmethod
import pandas as pd

class BaseSource(ABC):

    @abstractmethod
    def fetch(self, start_date: str, end_date: str) -> pd.DataFrame:
        pass