from unittest import mock
import pytest
import requests
from src.data.sources.OpenMeteoSource import OpenMeteoSource


def test_open_meteo_object_creation_good_args_and_place_name():
    open_meteo_object = OpenMeteoSource(columns=["temperature_2m"], weather_model="jma_seamless",
                                        prediction_horizon=7, place_name="Berlin")
    assert open_meteo_object.longitude
    assert open_meteo_object.latitude


def test_open_meteo_object_creation_good_args_and_wrong_place_name():
    with pytest.raises(requests.exceptions.RequestException):
        OpenMeteoSource(columns=["temperature_2m"], weather_model="jma_seamless",
                        prediction_horizon=7, place_name="asdsad")


def test_open_meteo_object_creation_columns_missing(capfd):
    with pytest.raises(ValueError):
        OpenMeteoSource(weather_model="jma_seamless", prediction_horizon=7, place_name="Berlin")
    out, err = capfd.readouterr()
    assert out == "Field 'columns' cannot be None\n"


def test_open_meteo_object_creation_longitude_latitude_place_missing(capfd):
    with pytest.raises(ValueError):
        OpenMeteoSource(columns=["temperature_2m"], weather_model="jma_seamless", prediction_horizon=7)
    out, err = capfd.readouterr()
    assert out == "Provide either place_name or latitude and longitude\n"


@mock.patch("requests.get", side_effect=requests.RequestException('Failed Request'))
def test_request_geocoding(mock_requests, capfd):
    with pytest.raises(SystemExit):
        OpenMeteoSource(columns=["temperature_2m"], weather_model="jma_seamless",
                        prediction_horizon=7, place_name="Berlin")
    mock_requests.assert_called_once()
    out, err = capfd.readouterr()
    assert out == "Error while converting place name to latitude/longitude\nFailed Request\n"


def test_creating_api_params():
    open_meteo_object = OpenMeteoSource(columns=["temperature_2m"], weather_model="jma_seamless",
                                        prediction_horizon=7, place_name="Berlin")
    params = open_meteo_object._create_api_params(start_date="20-01-2020", end_date="12-01-2021")
    assert type(params) == dict
    assert params["start_date"] == "20-01-2020"
    assert params["end_date"] == "12-01-2021"
    assert params["hourly"] == ["temperature_2m_previous_day" + str(i) for i in range(1, 8)]


def test_fetch_data_within_date_range_bad_dates(capfd):
    open_meteo_object = OpenMeteoSource(columns=["temperature_2m"], weather_model="jma_seamless",
                                        prediction_horizon=7, place_name="Berlin")
    with pytest.raises(SystemExit):
        open_meteo_object.fetch_data_within_date_range(start_date="20-01-2020", end_date="asdad")
        open_meteo_object.fetch_data_within_date_range(start_date="asdad", end_date="20-01-2020")
    out, err = capfd.readouterr()
    assert out == "Error parsing start or end date. Check dates!\n"


@mock.patch("openmeteo_requests.Client.Client.weather_api")
@mock.patch("src.data.sources.OpenMeteoSource.OpenMeteoSource._process_open_meteo_responses")
def test_fetch_data_within_date_range_good_dates(weather_api_mock, process_response_mock):
    open_meteo_object = OpenMeteoSource(columns=["temperature_2m"], weather_model="jma_seamless",
                                        prediction_horizon=7, place_name="Berlin")
    open_meteo_object.fetch_data_within_date_range(start_date="20-01-2020", end_date="21-01-2020")
    weather_api_mock.assert_called_once()
    process_response_mock.assert_called_once()

