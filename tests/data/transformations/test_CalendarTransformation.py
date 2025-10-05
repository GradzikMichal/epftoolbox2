import holidays
import pandas as pd
import holidays
import pydantic
import pytest
from src.data.transformations.CalendarTransformation import CalendarTransformation

file_path_good_file = "tests/files/test_data.csv"
file_path_bad_file = "tests/files/test.csv"
good_data = pd.read_csv(file_path_good_file, index_col=0, parse_dates=True)


def test_calendar_transformation_object_creation_all_good_arg():
    transformer = CalendarTransformation(weekly_dummies="one-hot", monthly_dummies="one-hot",
                                         quarterly_dummies="one-hot",
                                         country_code="PL", holidays_dummies="one-hot")
    assert transformer


def test_calendar_transformation_object_creation_no_arg():
    transformer = CalendarTransformation()
    assert transformer


def test_calendar_transformation_object_creation_wrong_arg():
    with pytest.raises(pydantic.ValidationError):
        CalendarTransformation(weekly_dummies="test")
        CalendarTransformation(monthly_dummies="test")
        CalendarTransformation(quarterly_dummies="test")
        CalendarTransformation(country_code=10)
        CalendarTransformation(holidays_dummies="test")


def test_calendar_transformation_transform_weekly_dummies_one_hot():
    transformer = CalendarTransformation(weekly_dummies="one-hot")
    transformed_data = transformer.transform(good_data)
    transformed_data_columns = transformed_data.columns

    assert (("is_Monday" in transformed_data_columns) and
            ("is_Tuesday" in transformed_data_columns) and
            ("is_Wednesday" in transformed_data_columns) and
            ("is_Thursday" in transformed_data_columns) and
            ("is_Friday" in transformed_data_columns) and
            ("is_Saturday" in transformed_data_columns) and
            ("is_Sunday" in transformed_data_columns))


def test_calendar_transformation_transform_weekly_dummies_list():
    transformer = CalendarTransformation(weekly_dummies="list")
    transformed_data = transformer.transform(good_data)
    transformed_data_columns = transformed_data.columns

    assert "weekday" in transformed_data_columns


def test_calendar_transformation_transform_weekly_dummies_number():
    transformer = CalendarTransformation(weekly_dummies="number")
    transformed_data = transformer.transform(good_data)
    transformed_data_columns = transformed_data.columns

    assert "weekday" in transformed_data_columns


def test_calendar_transformation_transform_weekly_dummies_list_number_comp():
    transformer_list = CalendarTransformation(weekly_dummies="list")
    transformed_data_list = transformer_list.transform(good_data.copy())
    transformer_number = CalendarTransformation(weekly_dummies="number")
    transformed_data_number = transformer_number.transform(good_data.copy())

    assert not transformed_data_list["weekday"].equals(transformed_data_number["weekday"])

def test_calendar_transformation_transform_holiday_dummies_one_hot():
    transformer = CalendarTransformation(country_code="PL", holidays_dummies="one-hot")
    transformed_data = transformer.transform(good_data.copy())
    transformed_data_columns = transformed_data.columns
    good_data_columns = good_data.columns
    assert len(transformed_data_columns) != len(good_data_columns)
    assert len(transformed_data_columns) == len(good_data_columns) + 1
    assert "is_nowy_rok" in transformed_data_columns

def test_calendar_transformation_transform_holiday_dummies_list():
    transformer = CalendarTransformation(country_code="PL", holidays_dummies="list")
    transformed_data = transformer.transform(good_data.copy())
    assert "holiday_name" in transformed_data.columns

def test_calendar_transformation_transform_holiday_dummies_number():
    transformer = CalendarTransformation(country_code="PL", holidays_dummies="number")
    transformed_data = transformer.transform(good_data.copy())
    assert "is_holiday" in transformed_data.columns
