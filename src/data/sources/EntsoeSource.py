import pandas as pd
from entsoe import EntsoePandasClient
from rich.progress import Progress

from src.data.sources.BaseSource import BaseSource


class EntsoeSource(BaseSource):

    def __init__(self, country_code: str, api_key: str):
        self.country_code = country_code
        self.api_key = api_key
        self.client = EntsoePandasClient(api_key=api_key)
        self.progress = Progress()

    def fetch(self, start_date, end_date) -> pd.DataFrame:
        self.progress.start()
        task = self.progress.add_task("[yellow]Entsoe Source", total=3)

        self.progress.console.log("[dim]Downloading load from ENTSOE")

        load = self.client.query_load(
            country_code=self.country_code,
            start=pd.Timestamp(start_date, tz='UTC'),
            end=pd.Timestamp(end_date, tz='UTC')
        ).rename({"Actual Load":"load"}, axis='columns').tz_convert('UTC')
        self.progress.update(task,advance=1)
        
        self.progress.console.log("[dim]Downloading prices from ENTSOE")
            
        prices = self.client.query_day_ahead_prices(
            country_code=self.country_code,
            start=pd.Timestamp(start_date, tz='UTC'),
            end=pd.Timestamp(end_date, tz='UTC')
        ).rename('price').tz_convert('UTC')
        
        self.progress.console.log("[dim]Postprocessing ENTSOE")
        self.progress.update(task,advance=1)
            
        load = load.resample('h').mean()
        
        merged = pd.merge(load, prices, left_index=True, right_index=True, how='left')
        
        merged.index.name="datetime"
    
        self.progress.console.log("[green]Entsoe downloaded successfully")
        self.progress.update(task,advance=1)
        self.progress.stop()
        return merged