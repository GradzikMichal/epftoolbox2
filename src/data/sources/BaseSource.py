from abc import ABC, abstractmethod
import pandas as pd


class BaseSource(ABC):

    @abstractmethod
    def fetch_data_within_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_data_within_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        pass
