import pandas as pd
from entsoe import EntsoePandasClient
from rich.progress import Progress

from src.data.sources.BaseSource import BaseSource


class CsvSource(BaseSource):

    def __init__(self, path: str, index: str):
        self.path = path
        self.index = index

    def fetch(self, start_date, end_date) -> pd.DataFrame:
        data = pd.read_csv(self.path)
        data.index=pd.to_datetime(data[self.index])
        return data.loc[start_date:end_date,:]