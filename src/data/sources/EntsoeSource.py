import pandas as pd
from entsoe import EntsoePandasClient
from rich.progress import Progress
from pydantic import BaseModel, ConfigDict, Field
from src.data.sources.BaseSource import BaseSource


class EntsoeSource(BaseSource, BaseModel):
    """
        A class used for fetching data from Entsoe database.

        :param country_code: Country code e.g. `'DE', 'PL'`
        :type country_code: str
        :param api_key: The api key for Entsoe API
        :type api_key: str
    """

    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    country_code: str = Field(strict=True, frozen=True, examples=["DE", "PL"],
                              description="Country code e.g. `'DE', 'PL'`")
    api_key: str = Field(strict=True, frozen=True, description="The api key for Entsoe API", )
    client: EntsoePandasClient = Field(frozen=True)
    progress: Progress = Field(default_factory=Progress)

    def __init__(self, country_code: str, api_key: str):
        super().__init__(
            country_code=country_code,
            api_key=api_key,
            client=EntsoePandasClient(api_key=api_key),
            progress=Progress()
        )

    def _fetch_load_within_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetches data from Entsoe database and returns it as a pandas dataframe.
        :param start_date: Starting date of data to be fetched.
        :type start_date: str
        :param end_date: Final date of data to be fetched.
        :type end_date: str
        :return: Pandas dataframe
        :rtype: pd.DataFrame
        """
        self.progress.console.log("[dim]Downloading load from ENTSOE")
        load = self.client.query_load(
            country_code=self.country_code,
            start=pd.Timestamp(start_date, tz='UTC'),
            end=pd.Timestamp(end_date, tz='UTC')
        ).rename({"Actual Load": "load"}, axis='columns').tz_convert('UTC')
        self.progress.console.log("[dim]Load download successful")
        load = load.resample('h').mean()
        return load

    def _fetch_price_within_date_range(self, start_date: str, end_date: str) -> pd.Series:
        """
            Fetches prices from Entsoe database and returns it as a pandas series.
            :param start_date: Starting date of data to be fetched.
            :type start_date: str
            :param end_date: Final date of data to be fetched.
            :type end_date: str
            :return: Pandas series
            :rtype: pd.Series
        """
        self.progress.console.log("[dim]Downloading prices from ENTSOE")
        prices = self.client.query_day_ahead_prices(
            country_code=self.country_code,
            start=pd.Timestamp(start_date, tz='UTC'),
            end=pd.Timestamp(end_date, tz='UTC')
        ).rename('price').tz_convert('UTC')
        self.progress.console.log("[dim]Price download successful")
        return prices

    def _combining_load_and_price(self, load: pd.DataFrame, price: pd.Series) -> pd.DataFrame:
        """
            Function for combining load and price.
            :param load: Dataframe containing load data.
            :type load: pd.DataFrame
            :param price: Series containing price data.
            :type price: str
            :return: Pandas dataframe containing combined load and price.
            :rtype: pd.Dataframe
        """
        self.progress.console.log("[dim]Combining ENTSOE prices and load")
        merged = pd.merge(load, price, left_index=True, right_index=True, how='left')
        merged.index.name = "datetime"
        return merged

    def fetch_data_within_date_range(self,
                                     start_date: str,
                                     end_date: str
                                     ) -> pd.DataFrame | pd.Series:
        """
            Fetches load and prices from Entsoe database and returns it as a pandas dataframe.
            :param start_date: Starting date of data to be fetched.
            :type start_date: str
            :param end_date: Final date of data to be fetched.
            :type end_date: str
            :return: Pandas dataframe
            :rtype: pd.Dataframe
        """

        self.progress.start()
        task = self.progress.add_task("[yellow]Downloading load and price data from Entsoe", total=3)

        load = self._fetch_load_within_date_range(start_date=start_date, end_date=end_date)
        self.progress.update(task, advance=1)

        price = self._fetch_price_within_date_range(start_date=start_date, end_date=end_date)
        self.progress.update(task, advance=1)

        data = self._combining_load_and_price(load, price)

        self.progress.console.log("[green]Load and price download successful")
        self.progress.update(task, advance=1)
        self.progress.stop()
        return data

    def fetch_data(self) -> pd.DataFrame:
        pass
