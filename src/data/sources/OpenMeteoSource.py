from typing import Literal, Optional, Tuple
import pandas as pd
from pandas.tseries.offsets import DateOffset
import requests
from rich.progress import Progress
from time import sleep
from pydantic import BaseModel, ConfigDict, Field
from src.data.sources.BaseSource import BaseSource


def _request_geocoding(place_name: str) -> Tuple[float, float]:
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
        geocoding = geocoding.json()
        longitude = geocoding['results'][0]['longitude']
        latitude = geocoding['results'][0]['latitude']
    return latitude, longitude


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

    model_config = ConfigDict(use_attribute_docstrings=True)
    columns: list[__columns_literal] = Field(frozen=True, strict=True, examples=[['cloud_cover'],
                                                                                 ["cloud_cover", "wind_speed_10m",
                                                                                  "wind_direction_10m"]])
    weather_model: __models_literal = Field(frozen=True, strict=True, examples=["jma_seamless"])
    prediction_horizon: int = Field(frozen=True, strict=True, examples=[1, 2, 3, 4, 7], ge=1, le=7)
    columns_prefix: Optional[str | None] = Field(None, frozen=True, examples=["prefix1", "prefix2", "prefix3"])
    longitude: Optional[float | str | None] = Field(examples=[21.0118], frozen=True, strict=True)
    latitude: Optional[float | str | None] = Field(examples=[52.2298], frozen=True, strict=True)

    def __init__(self,
                 latitude: Optional[float | str | None] = None,
                 longitude: Optional[float | str | None] = None,
                 prediction_horizon: int = 7,
                 weather_model: __models_literal = "jma_seamless",
                 columns: list[__columns_literal, ...] = None,
                 columns_prefix=None,
                 place_name: Optional[str | None] = None,
                 ):
        if columns is None:
            raise ValueError("Field 'columns' cannot be None")
        if place_name is None and latitude is None and longitude is None:
            raise ValueError("Provide either place_name or latitude and longitude")
        if place_name is not None:
            latitude, longitude = _request_geocoding(place_name)

        self.progress = Progress()
        super().__init__(columns=columns, weather_model=weather_model, columns_prefix=columns_prefix, latitude=latitude,
                         longitude=longitude,
                         prediction_horizon=prediction_horizon)

    def fetch(self, start_date, end_date) -> pd.DataFrame:
        self.progress.start()
        start_date = pd.Timestamp(start_date, tz='UTC')
        end_date = pd.Timestamp(end_date, tz='UTC') + DateOffset(days=self.horizon - 1)
        task = self.progress.add_task("[yellow]Open Meteo Source", total=1 + int(
            ((end_date.year - start_date.year) * 12 + end_date.month - start_date.month) / 3))
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": [f'{column}_previous_day{i + 1}' for i in range(self.horizon) for column in self.columns],
            "models": self.model,
            "timezone": "GMT"
        }
        weather = {}
        self.progress.console.log("[dim]Downloading weather data from Open Meteo")

        while start_date < end_date:
            params["start_date"] = start_date.strftime("%Y-%m-%d")
            params["end_date"] = min((start_date + DateOffset(months=3)), end_date).strftime("%Y-%m-%d")
            try:
                response = requests.get('https://previous-runs-api.open-meteo.com/v1/forecast', params)
            except requests.RequestException as e:
                self.progress.console.log(f"[red]Error fetching data from Open Meteo: {e}")
                sleep(10)
                continue

            try:
                response = response.json()
            except ValueError:
                self.progress.console.log("[red]Error fetching data from Open Meteo")
                sleep(10)
                continue

            if 'error' in response and response['error'] == True:
                self.progress.console.log(f"[red]Error fetching data from Open Meteo: {response['reason']}")
                if response['reason'] == "Minutely API request limit exceeded. Please try again in one minute.":
                    self.progress.console.log(
                        "[dim][yellow] Minutely API request limit exceeded. Resuming in one minute.")
                    sleep(60)
                if response['reason'] == "Too many concurrent requests":
                    sleep(10)
                    self.progress.console.log("[dim][yellow] Too many concurrent requests. Resuming in one 10 sec.")
                continue

            for x, y in enumerate(response['hourly']['time']):
                try:
                    tmp = {}
                    for i in range(1, self.horizon + 1):
                        for column in self.columns:
                            tmp[f'{column}_d+{i}'] = response['hourly'][f'{column}_previous_day{i}'][x + (i * 24)]
                    # if we want to limit the forecasts to 7 days then we need to use pd.Timestamp.now(tz='UTC') or round to the whole day 
                    if pd.Timestamp(y, tz='UTC') > (pd.Timestamp.now(tz='UTC').floor('h') + DateOffset(hours=13)):
                        continue
                    weather[y] = tmp
                except:
                    pass
            start_date = start_date + DateOffset(months=3) - DateOffset(days=self.horizon)
            self.progress.update(task, advance=1)

        self.progress.console.log("[dim]Postprocessing Open Meteo")

        weather = pd.DataFrame.from_dict(weather, orient='index_col')
        weather.index.name = "datetime"
        weather.index = pd.to_datetime(weather.index).tz_localize('UTC')
        if self.columns_prefix:
            weather.columns = [f"{self.prefix}_{col}" for col in weather.columns]

        self.progress.update(task)
        self.progress.console.log("[green]Open Meteo downloaded successfully")
        self.progress.stop()
        return weather
