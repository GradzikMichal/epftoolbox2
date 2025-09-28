from typing import Literal

import pandas as pd
import numpy as np
from rich import print
from pydantic import BaseModel, Field, ConfigDict
from src.data.transformations.BaseTransformation import BaseTransformation


class LagTransformation(BaseTransformation, BaseModel):
    __types = Literal["day", "hour"]

    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    columns: list[str] = Field(strict=True, frozen=True, default_factory=list,
                               description="List of column names to add lag to.")
    lags: list[int] = Field(strict=True, frozen=True, default_factory=list, description="List of lags ")
    lag_type: __types = Field(frozen=True, default_factory=list, description="Type of lag", example=["day", "hour"])

    def __init__(
            self, columns: list[str] = list, lags: list[int] = list, lag_type: __types = 'day'):
        super().__init__(
            columns=columns, lags=lags, lag_type=lag_type
        )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        print(f"[dim]Transforming lags {self.columns}")
        new_columns = {}
        for column in self.columns:
            for lag in self.lags:
                lagged_column_name = f"{column}_{"d" if self.type == 'day' else 'h'}{'-' if lag >= 0 else '+'}{abs(lag)}"
                shift_amount = lag * (24 if self.type == 'day' else 1)
                new_columns[lagged_column_name] = data[column].shift(shift_amount)

        lagged_df = pd.DataFrame(new_columns)
        result = pd.concat([data, lagged_df], axis=1)

        print(f"[green]Lag transformation successful")
        return result
