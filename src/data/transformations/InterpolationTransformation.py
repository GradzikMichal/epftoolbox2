from typing import Literal
import pandas as pd
from rich import print
from pydantic import BaseModel, Field, ConfigDict
from src.data.transformations.BaseTransformation import BaseTransformation


class InterpolationTransformation(BaseTransformation, BaseModel):
    """
        Class which allows one to apply an interpolation to the columns of dataset.
        :param interpolation_type: The type of interpolation to apply to the dataset columns.
        :type interpolation_type: str

    """
    __interpolation_type = Literal["linear", "first"]

    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    interpolation_type: __interpolation_type = Field(strict=True, frozen=True,
                                                     description="The interpolation method used for the transformation of a data.")

    def __init__(self, interpolation_type: str = "linear"):
        super().__init__(
            interpolation_type=interpolation_type
        )

    def transform(self, data: pd.DataFrame, resample: str = "h", round_data: int = -1) -> pd.DataFrame:
        """
            Transforms the given data using the given interpolation method. Index of a dataframe must be a date column.


            :param data: Data to be transformed.
            :type data: pd.DataFrame
            :param resample: Defines how to resample the data. Default is "h" (horizontally).
            :type resample: str
            :param round_data: Round the data. Default is -1 which defines no rounding.
            :type round_data: int
            :return: Returns transformed data.
            :rtype: pd.DataFrame
        """
        data = data.resample(resample)
        match self.interpolation:
            case "linear":
                data = data.mean().interpolate(method='linear')
            case "first":
                data = data.first().ffill()
            case _:
                raise ValueError(f"Interpolation method {self.interpolation_type} is not supported.")
        if round_data >= 0:
            data = data.round(round_data)
        print(f"[green]Interpolation transformation successful")
        return data
