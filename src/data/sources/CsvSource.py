from typing import Hashable

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from .BaseSource import BaseSource


class CsvSource(BaseSource, BaseModel):
    """
    A class used for fetching data from csv files.

    :param file_path: The path to the csv file e.g. `'path/to/file.csv'`
    :type file_path: str
    :param index_col: The index_col of the csv file e.g. `'index_col'`. If None index column is chosen automatically.
    :type index_col: str | int | None
    :param date_col: The name of the column in the csv file e.g. `'date'`. Default is `'date'`
    :type date_col: str
    """
    model_config = ConfigDict(use_attribute_docstrings=True)
    file_path: str = Field(strict=True, frozen=True, examples=["path/to/file.csv"],
                           description="The path to the csv file e.g. `'path/to/file.csv'`")
    index_col: str | int | None = Field(frozen=True, examples=['date'],
                                  description="The index_col of the csv file e.g. `'index_col'`")
    date_col: str | None = Field(strict=True, frozen=True, examples=['date'],
                                 description="The name of the column in the csv file e.g. `'date'`")

    def __init__(self, file_path: str, index_col: Hashable | None, date_col: str = "date"):
        super().__init__(file_path=file_path, index_col=index_col, date_col=date_col)

    def fetch_data(self) -> pd.DataFrame:
        """
        Method for fetching full data from csv files.
        :return: A pandas DataFrame containing data from csv files.
        :rtype: pd.DataFrame
        """
        try:
            data = pd.read_csv(self.file_path, parse_dates=True, index_col=self.index_col)
        except FileNotFoundError as e:
            print(e)
            print("[red]Error while reading file! File not found[/red]")
            exit(1)
        return data

    def fetch_data_within_date_range(self, start_date: str, end_date: str, data_type=None) -> pd.DataFrame:
        """
        Method for fetching data from csv files within given daterange.
        :param start_date: The start date of the data range. Included in returned Dataframe.
        :type start_date: str
        :param end_date: The end date of the data range. Included in returned Dataframe.
        :type end_date: str
        :param data_type: Not used, defaults to None
        :type data_type:
        :return: Pandas DataFrame with data within given daterange.
        :rtype: pd.DataFrame
        """
        data = self.fetch_data()
        if self.date_col == self.index_col:
            data = data[(data.index >= start_date) & (data.index <= end_date)]
        else:
            data = data[data[self.date_col].between(start_date, end_date)]
        return data
