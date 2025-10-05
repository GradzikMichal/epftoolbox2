from typing import Literal
import pandas as pd
from rich import print
from pydantic import BaseModel, Field, ConfigDict
from src.data.transformations.BaseTransformation import BaseTransformation

"TODO: This class is weird... It is doing date changes and interpolation - split this class?"


class TimeZoneTransformation(BaseTransformation, BaseModel):
    """
        Class which allows one to apply an interpolation to the columns of dataset.
        :param timezone: The timezone to apply to the date column.
        :type timezone: str
        :param gmt_column: If true adds GMT offset column to the dataset column.
        :type gmt_column: bool
    """
    __interpolation_type = Literal["linear", "first", None]

    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    timezone: str | None = Field(strict=True, frozen=True,
                                 description="The timezone used for the transformation of a date column.")
    gmt_column: bool = Field(frozen=True, strict=True, description="Whether the date column is GMT.", default=False)

    def __init__(self, timezone: str | None, gmt_column: bool = True):
        super().__init__(
            timezone=timezone, gmt_column=gmt_column
        )

    def transform(self, data: pd.DataFrame, create_index: bool = False,
                  datetime_column: str = "datetime") -> pd.DataFrame:
        """
            Transforms the given data datetime column using the given timezone and GMT column.
            :param data: Data to be transformed.
            :type data: pd.DataFrame
            :param create_index: Determines whether to create the datetime index.
            :type create_index: bool
            :param datetime_column: Determines which column in data is datetime column.
            :type datetime_column: str
            :return: Returns transformed data.
            :rtype: pd.DataFrame
        """
        if create_index:
            data.index = pd.to_datetime(data[datetime_column].strftime('%Y-%m-%d %H:%M'))
        if self.timezone is not None:
            print(f"[dim]Converting to timezone {self.timezone}")
            data[self.timezone] = data.tz_convert(self.timezone)
        if self.gmt_column:
            data['gmt_offset'] = data.index.map(lambda x: int(x.utcoffset().total_seconds() / 3600))
        print(f"[green]Timezone transformation successful")
        return data
