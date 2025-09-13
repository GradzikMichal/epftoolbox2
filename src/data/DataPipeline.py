import pandas as pd
from ..data.transformations.BaseTransformation import BaseTransformation
from ..data.sources.BaseSource import BaseSource
from rich import print


class DataPipeline:
    """
    A class used for fetching and transforming data.

    :param start_date: The start date of the data in format YYYY-MM-DD.
    :type start_date: str
    :param end_date: The end date of the data in format YYYY-MM-DD.
    :type end_date: str
    :param sources: The list of data sources from which download data.
        Available sources: [CsvSource, EntsoeSource, OpenMeteoSource]
    :type sources: List[BaseSource] | None, optional
    :param transformations:
        The list of transformations to perform on data.
        Available transformations: [CalendarTransformation, LagTransformation, TimeZoneTransformation]
    :type transformations: List[BaseTransformation] | None, optional
    """

    def __init__(
            self, start_date: str,
            end_date: str,
            sources: list[BaseSource] | list = list,
            transformations: list[BaseTransformation] | list = list,
    ) -> None:
        super().__init__()
        self.__start_date: str = start_date
        self.__end_date: str = end_date
        self.__sources: list[BaseSource] = sources
        self.__transformations: list[BaseTransformation] = transformations

    def get_data(self, use_saved: bool = False, file_name: str = None) -> pd.DataFrame:
        """
            Function which fetches data from data source.
            :param use_saved: Whether to used save data or not. Used with `file_name`.
            :type use_saved: bool
            :param file_name: The name of the file to load the data from or save the data to.
            :type file_name: str
            :return: A dataframe containing the data from the data source.
        """
        if use_saved and file_name is not None:
            try:
                return pd.read_csv(f'{file_name}', index_col=0)
            except FileNotFoundError as e:
                print(e)
                print("[red]Error while reading cached file! File not found[/red]")
                exit(1)
        data = pd.DataFrame()
        for source in self.__sources:
            df = source.fetch(self.__start_date, self.__end_date)
            data = pd.merge(data, df, left_index=True, right_index=True, how='outer')
        return data

    def get_transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
            Function which transforms data using given transformations.
            :param data: Whether to save the data or not. Used with `file_name`.
            :type data: pd.DataFrame
            :return: A dataframe containing the transformed data.
        """
        for transformation in self.__transformations:
            data = transformation.transform(data)
        return data

    def get_transform_data(self, save_data: bool = False, use_saved: bool = False,
                           file_name: str = None) -> pd.DataFrame:
        """
        Function which fetches data from data source and transforms it using given transformations.
        :param save_data: Whether to save the data or not. Used with `file_name`.
        :type save_data: bool
        :param use_saved: Whether to used save data or not. Used with `file_name`.
        :type use_saved: bool
        :param file_name: The name of the file to load the data from or save the data to.
        :type file_name: str
        :return: A dataframe containing the transformed data from the sources.
        """
        data = self.get_data(use_saved=use_saved, file_name=file_name)
        data = self.get_transform(data)

        if save_data and file_name is not None:
            data.to_csv(f'{file_name}.csv')

        return data
