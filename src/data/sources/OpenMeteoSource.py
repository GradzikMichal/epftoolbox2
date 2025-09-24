from typing import Literal, Optional, Tuple
import pandas as pd
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse
import requests
from rich.progress import Progress
from pydantic import BaseModel, ConfigDict, Field
from src.data.sources.BaseSource import BaseSource
import openmeteo_requests
from dateutil.parser import parse, ParserError
import requests_cache
from retry_requests import retry


class OpenMeteoSource(BaseSource, BaseModel):
    """
        A class used for fetching data from OpenMeteo database.

        :param columns: List of columns which fetch from API e.g. `["cloud_cover", "wind_speed_10m", "wind_direction_10m"]`. Required.
        :type columns: list[__columns_literal]
        :param weather_model: Name of weather model to use e.g. `"jma_seamless"`. Required.
        :type weather_model: __models_literal
        :param prediction_horizon: The number of forecasted days  e.g. `7`. Default is `7`
        :type prediction_horizon: int
        :param prediction_horizon: The number of forecasted days  e.g. `7`. Default is `7`
        :type prediction_horizon: int
        :param columns_prefix: Prefix to add to  columns names  e.g. `prefix_`. Default is `None`
        :type columns_prefix: int
        :param longitude: Longitude of the place which weather data is fetched. If not provided, ``place_name`` is used to get ``longitude``. Default is `None`.
        :type longitude: int
        :param latitude: Latitude of the place which weather data is fetched. If not provided, ``place_name`` is used to get ``latitude``. Default is `None`.
        :type latitude: Optional[float | str | None]
        :param place_name: Name of place used to get ``longitude`` and ``latitude``. Default is `None`. Not a field.
        :type place_name: Optional[float | str | None]
    """

    __columns_literal = Literal[
        "temperature_2m", "rain", "showers", "snowfall", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
        "precipitation", "weather_code", "surface_pressure", "pressure_msl", "cloud_cover", "wind_speed_10m", "wind_direction_10m"
    ]
    __models_literal = Literal["jma_seamless",]

    __default_url: str = "https://previous-runs-api.open-meteo.com/v1/forecast"

    model_config = ConfigDict(use_attribute_docstrings=True, arbitrary_types_allowed=True)
    columns: list[__columns_literal] = Field(frozen=True, examples=[['cloud_cover'],
                                                                    ["cloud_cover", "wind_speed_10m",
                                                                     "wind_direction_10m"]])
    weather_model: __models_literal = Field(frozen=True, examples=["jma_seamless"], default="jma_seamless")
    prediction_horizon: int = Field(frozen=True, strict=True, examples=[1, 2, 3, 4, 7], ge=1, le=7)
    columns_prefix: Optional[str | None] = Field(None, frozen=True, examples=["prefix1", "prefix2", "prefix3"])
    longitude: Optional[float | str | None] = Field(examples=[21.0118], frozen=True)
    latitude: Optional[float | str | None] = Field(examples=[52.2298], frozen=True)
    url: Optional[str] = Field(strict=True, frozen=True, default=__default_url,
                               examples=[__default_url])
    progress: Progress = Field(default_factory=Progress, frozen=True)

    def __init__(self,
                 latitude: Optional[float | str | None] = None,
                 longitude: Optional[float | str | None] = None,
                 prediction_horizon: int = 7,
                 weather_model: __models_literal = "jma_seamless",
                 columns: list[__columns_literal] = None,
                 columns_prefix=None,
                 place_name: Optional[str | None] = None,
                 url: Optional[str] = __default_url,
                 ):
        if columns is None:
            print("Field 'columns' cannot be None")
            raise ValueError()
        if place_name is None and latitude is None and longitude is None:
            print("Provide either place_name or latitude and longitude")
            raise ValueError()
        if place_name is not None:
            longitude, latitude = self.request_geocoding(place_name)
        super().__init__(columns=columns, weather_model=weather_model, columns_prefix=columns_prefix,
                         latitude=latitude, longitude=longitude, prediction_horizon=prediction_horizon, url=url)

    def request_geocoding(self, place_name) -> (float, float):
        """
            Function for requesting translation of place name like 'Berlin' to latitude and longitude.
            :param place_name: Name of the place to geocode.
            :type place_name: str
            :return: Tuple of latitude and longitude.
            :rtype: Tuple[float, float]
        """
        try:
            geocoding = requests.get(
                url="https://geocoding-api.open-meteo.com/v1/search",
                params={
                    "name": place_name,
                    "count": 1
                })
        except requests.RequestException as e:
            print("Error while converting place name to latitude/longitude")
            print(e)
            exit(1)
        else:
            if "results" in geocoding.json().keys():
                geocoding = geocoding.json()
                return geocoding['results'][0]['longitude'], geocoding['results'][0]['latitude']
            else:
                Progress().console.log("[red]Wrong place name!")
                raise requests.exceptions.RequestException()

    def _create_api_params(self, start_date: str, end_date: str):
        hourly_columns = [column + "_previous_day" + str(i) for column in self.columns for i in
                          range(1, self.prediction_horizon + 1)]
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": hourly_columns,
            "start_date": start_date,
            "end_date": end_date,
        }

    def _process_open_meteo_responses(self, responses: list[WeatherApiResponse]) -> pd.DataFrame:
        hourly = responses[0].Hourly()
        data = {
            "datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit='s', utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit='s', utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }
        for i, column_name in enumerate(self.columns):
            data[column_name] = hourly.Variables(i).ValuesAsNumpy()
        return pd.DataFrame(data=data)

    def fetch_data_within_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            parse(start_date)
            parse(end_date)
        except ParserError:
            print("Error parsing start or end date. Check dates!")
            exit(1)
        self.progress.start()
        task = self.progress.add_task("[yellow]Downloading weather data from Open-Meteo Source", total=2)
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        open_meteo_client = openmeteo_requests.Client(session=retry_session)
        api_params = self._create_api_params(start_date, end_date)
        responses: list[WeatherApiResponse] = open_meteo_client.weather_api(self.url, params=api_params)
        self.progress.console.log("[green]Open Meteo downloaded successfully")
        self.progress.update(task, advance=1)
        self.progress.console.log("[yellow]Processing response from Open Meteo")
        processed_response = self._process_open_meteo_responses(responses)
        self.progress.console.log("[green]Response processed successfully")
        self.progress.update(task, advance=1)
        return processed_response
