from abc import ABC, abstractmethod
import pandas as pd


class BaseSource(ABC):

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        pass
