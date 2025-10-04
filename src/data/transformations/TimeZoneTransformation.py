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
        :param interpolation: The type of interpolation to apply to the dataset columns.
        :type interpolation: str
        :param gmt_column: If true adds GMT offset column to the dataset column.
        :type gmt_column: bool
    """
    __interpolation_type = Literal["linear", "first", None]

    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    timezone: str | None = Field(strict=True, frozen=True,
                                 description="The timezone used for the transformation of a date column.")
    interpolation: __interpolation_type = Field(strict=True, frozen=True,
                                                description="The interpolation method used for the transformation of a data.")
    gmt_column: bool = Field(frozen=True, strict=True, description="Whether the date column is GMT.", default=False)

    def __init__(self, timezone: str | None, interpolation: str = "linear", gmt_column: bool = True):
        super().__init__(
            timezone=timezone, interpolation=interpolation, gmt_column=gmt_column
        )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
            Transforms the given data using the given interpolation method. Index of a dataframe must be a date column.
            :param data: Data to be transformed.
            :type data: pd.DataFrame
            :return: Returns transformed data.
            :rtype: pd.DataFrame
        """
        data.index = pd.to_datetime(data.index.strftime('%Y-%m-%d %H:%M'))
        data['utc_datetime'] = data.index
        if self.timezone is not None:
            print(f"[dim]Converting to timezone {self.timezone}")
            data = data.tz_convert(self.timezone)
        if self.gmt_column:
            data['gmt_offset'] = data.index.map(lambda x: int(x.utcoffset().total_seconds() / 3600))
        if self.interpolation is not None:
            data = data.resample('h')
            match self.interpolation:
                case "linear":
                    data = data.mean().interpolate(method='linear')
                case "first":
                    data = data.first().ffill()
        data = data.round(2)
        print(f"[green]Timezone transformation successful")
        return data
