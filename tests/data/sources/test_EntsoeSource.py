from unittest import mock
import logging

import pandas as pd
import pydantic
import pytest

from src.data.sources.EntsoeSource import EntsoeSource


def test_entsoe_source_creation_good_type_args():
    assert EntsoeSource(country_code="PL",
                        api_key="test_api"
                        )


def test_entsoe_source_creation_missing_args():
    with pytest.raises(TypeError):
        EntsoeSource(country_code="PL", )


def test_entsoe_source_creation_wrong_args():
    with pytest.raises(pydantic.ValidationError):
        EntsoeSource(country_code="PL", api_key=True)


@mock.patch("src.data.sources.EntsoeSource.EntsoeSource._fetch_load_within_date_range")
@mock.patch("src.data.sources.EntsoeSource.EntsoeSource._fetch_price_within_date_range")
@mock.patch("src.data.sources.EntsoeSource.EntsoeSource._combining_load_and_price")
def test_entsoe_fetch_working(fetch_load_mock, fetch_price_mock, combining_load_and_price_mock):
    entsoe_source = EntsoeSource(country_code="PL", api_key="test_api")
    entsoe_source.fetch_data_within_date_range(start_date="2020-01-01", end_date="2020-02-01")
    fetch_load_mock.assert_called_once()
    fetch_price_mock.assert_called_once()
    combining_load_and_price_mock.assert_called_once()





@mock.patch("entsoe.EntsoePandasClient.query_load")
def test_entsoe_fetch_load_within_date_range(query_load_mock):
    entsoe_source = EntsoeSource(country_code="PL", api_key="test_api")
    entsoe_source._fetch_load_within_date_range(start_date="2020-01-01", end_date="2020-02-01")
    query_load_mock.assert_called_once()



@mock.patch("entsoe.EntsoePandasClient.query_day_ahead_prices")
def test_entsoe_fetch_price_within_date_range(day_ahead_mock):
    entsoe_source = EntsoeSource(country_code="PL", api_key="test_api")
    entsoe_source._fetch_price_within_date_range(start_date="2020-01-01", end_date="2020-02-01")
    day_ahead_mock.assert_called_once()


def test_entsoe_combining_load_and_price():
    entsoe_source = EntsoeSource(country_code="PL", api_key="test_api")
    load = pd.DataFrame({"datetime": ["2020-01-01", "2020-02-01"], "load": [1, 2]})
    price = pd.Series([1, 2], name="price")
    merged = entsoe_source._combining_load_and_price(load, price)
    assert isinstance(merged, pd.DataFrame)
