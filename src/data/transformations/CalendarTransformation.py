import pandas as pd
import holidays
from typing import Literal, Sequence
from rich.progress import Progress
from pydantic import BaseModel, Field, ConfigDict
from src.data.transformations.BaseTransformation import BaseTransformation
import calendar


class CalendarTransformation(BaseTransformation, BaseModel):
    """
    Class used to add additional calendar information to data.
    :param country_code: Code of a country used for getting holidays information
    :type country_code: str | None
    :param weekly_dummies: If provided, determines how code the weekly information. Used with `holidays_dummies`. Example: ["one-hot", "list", "number", None]
    :type weekly_dummies: str
    :param monthly_dummies: If provided, determines how code the monthly information. Example: ["one-hot", "list", "number", None]
    :type monthly_dummies: str
    :param quarterly_dummies: If provided, determines how code the quarterly information. Example: ["one-hot", "list", "number", None]
    :type quarterly_dummies: str
    :param holidays_dummies: If provided, determines how code the holidays' information. Used with `country_code`. Example: ["one-hot", "list", "number", None]
    :type holidays_dummies: str
    """
    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    __dummies_type = Literal["one-hot", "list", "number", None]
    weekly_dummies: __dummies_type = Field(frozen=True,
                                           description="If provided, determines how code the weekly information.",
                                           examples=["one-hot", "list", "number", None], default=None)
    monthly_dummies: __dummies_type = Field(frozen=True,
                                            description="If provided, determines how code the monthly information.",
                                            examples=["one-hot", "list", "number", None], default=None)
    quarterly_dummies: __dummies_type = Field(frozen=True,
                                              description="If provided, determines how code the quarterly information.",
                                              examples=["one-hot", "list", "number", None], default=None)
    country_code: str | None = Field(strict=True, frozen=True,
                                     description="Code of a country used for getting holidays information. Used with `holidays_dummies`.",
                                     examples=["DE", "PL"])
    holidays_dummies: __dummies_type = Field(frozen=True,
                                             description="If provided, determines how code the holidays information. Used with `country_code`.",
                                             examples=["one-hot", "list", "number", None], default=None)
    progress: Progress = Field(default_factory=Progress)

    def __init__(
            self, weekly_dummies: __dummies_type = 'one-hot', monthly_dummies: __dummies_type = None,
            quarterly_dummies: __dummies_type = None, country_code: str = "", holidays_dummies: __dummies_type = None
    ):
        super().__init__(
            weekly_dummies=weekly_dummies, monthly_dummies=monthly_dummies, quarterly_dummies=quarterly_dummies,
            country_code=country_code, holidays_dummies=holidays_dummies, progress=Progress()
        )

    def _create_is_list(self, names_list: Sequence[str]) -> list[str]:
        """
            Function for appending a string to the list of strings.
            :param names_list: List of strings to append.
            :type names_list: Sequence[str]
            :return: List of appended strings.
            :rtype: list[str]
        """
        return ["is_" + name for name in names_list]

    def _transform_template(
            self,
            data: pd.DataFrame,
            dummy_type: __dummies_type,
            one_hot_list: list[str] | Sequence[str] | None = None,
            comparison_data: pd.Series | None = None,
            column_name: str | None = None,
            list_column_data: pd.Series | None = None,
            number_column_data: pd.Series | None = None,
    ) -> pd.DataFrame:
        """
            Private function for adding column/columns containing additional information.
            :param data: User data to be transformed.
            :type data: pd.DataFrame
            :param dummy_type: Type of transformation.
            :type dummy_type: __dummies_type
            :param one_hot_list: List of columns name for `one-hot transformation`. Optional.
            :type one_hot_list: list[str]
            :param comparison_data: Data used for `one-hot transformation`. Optional.
            :type comparison_data: pd.Series
            :param column_name: Name of additional column. Used for `list and number transformation`. Optional.
            :type column_name: str
            :param list_column_data: Data used for `list transformation`. Optional.
            :type list_column_data: pd.Series
            :param number_column_data: Data used for `number transformation`. Optional.
            :type number_column_data: pd.Series
            :return: Returns pandas DataFrane with added columns.
            :rtype: pd.DataFrame
        """
        if dummy_type is not None:
            match dummy_type:
                case "one-hot":
                    for i, one_hot_name in enumerate(self._create_is_list(one_hot_list)):
                        data[one_hot_name] = (comparison_data == i + 1).astype(int)
                case 'list':
                    data[column_name] = list_column_data
                case "number":
                    data[column_name] = number_column_data
        return data

    def _transform_holidays(self, data: pd.DataFrame) -> pd.DataFrame:
        """
            Adding column/columns containing information about the holidays.
            :param data: User data to be transformed.
            :type data: pd.DataFrame
            :return: Returns pandas DataFrane with added columns.
            :rtype: pd.DataFrame
        """
        if self.holidays_dummies is not None:
            holiday_names: pd.Series = data.index.to_series().apply(
                lambda x: holidays.country_holidays(self.country_code, language="en_US").get(x))
            match self.holidays_dummies:
                case "one-hot":
                    unique_holidays = holiday_names.dropna().unique()
                    for holiday in unique_holidays:
                        data[f"is_{holiday.lower().replace(' ', '_')}"] = (holiday_names == holiday).astype(int)
                case 'list':
                    data['holiday_name'] = holiday_names.fillna('')
                case "number":
                    data['is_holiday'] = (holiday_names.notna()).astype(int)
        return data

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the given data, if ``weekly_dummies``, ``monthly_dummies``, ``quarterly_dummies`` or/and ``holidays_dummies``are provided as parameters in object constructor.
        :param data: User data to be transformed.
        :type data: pd.DataFrame
        :return: Returns pandas DataFrane with added transformations.
        :rtype: pd.DataFrame
        """
        self.progress.start()
        task = self.progress.add_task("[yellow]Adding calendar information", total=4)

        data = self._transform_template(
            data=data,
            dummy_type=self.weekly_dummies,
            one_hot_list=calendar.day_name,
            comparison_data=data.index.dayofweek + 1,
            column_name="weekday",
            list_column_data=data.index.day_name(),
            number_column_data=data.index.dayofweek + 1
        )
        self.progress.update(task, advance=1)
        data = self._transform_template(
            data=data,
            dummy_type=self.monthly_dummies,
            one_hot_list=calendar.month_name,
            comparison_data=data.index.month,
            column_name="month",
            list_column_data=data.index.month_name(),
            number_column_data=data.index.month
        )
        self.progress.update(task, advance=1)

        data = self._transform_template(
            data=data,
            dummy_type=self.quarterly_dummies,
            one_hot_list=['is_q1', 'is_q2', 'is_q3', 'is_q4'],
            comparison_data=data.index.quarter,
            column_name="quarter",
            list_column_data=data.index.quarter,
            number_column_data=data.index.quarter
        )

        self.progress.update(task, advance=1)

        data = self._transform_holidays(data)

        # data.set_index('datetime', inplace=True)
        # data.index_col.name="datetime"
        self.progress.update(task, advance=1)
        self.progress.console.log("[green]Calendar data added successfully")
        self.progress.stop()

        return data
