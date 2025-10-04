from typing import Literal
import pandas as pd
from rich import print
from pydantic import BaseModel, Field, ConfigDict
from src.data.transformations.BaseTransformation import BaseTransformation


class LagTransformation(BaseTransformation, BaseModel):
    """
        Class which allows one to apply a lag to the columns of dataset.
        :param columns: The list of columns to apply the lag to.
        :type columns: list[str]
        :param lags: The list of lags to apply to the columns of dataset.
        :type lags: list[int]
        :param lag_type: The type of lag to apply to the columns of dataset. Right now it supports only `day` and `hour`.
        :type lag_type: str
    """
    __lag_type = Literal["day", None]

    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    columns: list[str] = Field(strict=True, frozen=True, default_factory=list,
                               description="List of column names to add lag to.")
    lags: list[int] = Field(strict=True, frozen=True, default_factory=list, description="List of lags ")
    lag_type: __lag_type = Field(frozen=True, default_factory=list, description="Type of lag", example=["day", "hour"])

    def __init__(
            self, columns: list[str] = list, lags: list[int] = list, lag_type: __lag_type = 'day'):
        super().__init__(
            columns=columns, lags=lags, lag_type=lag_type
        )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Method to apply lags to the specified data columns. Right now it supports only `day`.
        :param data: Data which includes columns to apply lags to.
        :type data: pd.DataFrame
        :return: Returns data with additional lag columns.
        :rtype: pd.DataFrame
        """
        print(f"[dim]Adding lags to the columns: {self.columns}")
        new_columns = {}
        for column in self.columns:
            if column not in data.columns:
                for lag in self.lags:
                    lagged_column_name = f"{column}_{"d" if self.type == 'day' else 'h'}{'-' if lag >= 0 else '+'}{abs(lag)}"
                    shift_amount = lag * (24 if self.type == 'day' else 1)
                    new_columns[lagged_column_name] = data[column].shift(shift_amount)
            else:
                print(f"[yellow] Column {column} already is not in the data.")

        lagged_df = pd.DataFrame(new_columns)
        result = pd.concat([data, lagged_df], axis=1)

        print(f"[green]Lags added successfully")
        return result
