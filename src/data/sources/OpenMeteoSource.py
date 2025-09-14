import pandas as pd
from pandas.tseries.offsets import DateOffset
import requests
from rich.progress import Progress
from time import sleep

from src.data.sources.BaseSource import BaseSource


class OpenMeteoSource(BaseSource):
    columns = [
        "temperature_2m",
        "rain",
        "showers",
        "snowfall",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "precipitation",
        "weather_code",
        "surface_pressure",
        "pressure_msl",
        "cloud_cover",
        "wind_speed_10m",
        "wind_direction_10m"
        ]
    
    def __init__(self,latitude: float, longitude: float,horizon:int=7,model:str="jma_seamless",columns=[],prefix=""):
        self.latitude = latitude
        self.longitude = longitude
        self.prefix = prefix
        self.horizon = horizon
        self.model = model
        self.columns = columns if columns else self.columns
        self.progress = Progress()

    def fetch(self, start_date, end_date) -> pd.DataFrame:
        self.progress.start()
        start_date = pd.Timestamp(start_date, tz='UTC')
        end_date = pd.Timestamp(end_date, tz='UTC') + DateOffset(days=self.horizon-1)
        task = self.progress.add_task("[yellow]Open Meteo Source", total=1+int(((end_date.year-start_date.year)*12+end_date.month-start_date.month)/3))
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": [f'{column}_previous_day{i+1}' for i in range(self.horizon) for column in self.columns],
            "models": self.model,
            "timezone":"GMT"
        }
        weather={}
        self.progress.console.log("[dim]Downloading weather data from Open Meteo")

        while start_date<end_date:
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
            
            if 'error' in response and response['error']==True:
                self.progress.console.log(f"[red]Error fetching data from Open Meteo: {response['reason']}")
                if response['reason']=="Minutely API request limit exceeded. Please try again in one minute.":
                    self.progress.console.log("[dim][yellow] Minutely API request limit exceeded. Resuming in one minute.")
                    sleep(60)
                if response['reason']=="Too many concurrent requests":
                    sleep(10)
                    self.progress.console.log("[dim][yellow] Too many concurrent requests. Resuming in one 10 sec.")
                continue

            for x,y in enumerate(response['hourly']['time']):
                try:
                    tmp={}
                    for i in range(1,self.horizon+1):
                        for column in self.columns:
                            tmp[f'{column}_d+{i}']=response['hourly'][f'{column}_previous_day{i}'][x+(i*24)]
                    # if we want to limit the forecasts to 7 days then we need to use pd.Timestamp.now(tz='UTC') or round to the whole day 
                    if pd.Timestamp(y, tz='UTC')>(pd.Timestamp.now(tz='UTC').floor('h')+DateOffset(hours=13)):
                        continue
                    weather[y]=tmp
                except:
                    pass
            start_date = start_date + DateOffset(months=3) - DateOffset(days=self.horizon)
            self.progress.update(task,advance=1)

        
        self.progress.console.log("[dim]Postprocessing Open Meteo")

        weather = pd.DataFrame.from_dict(weather, orient='index')
        weather.index.name = "datetime"
        weather.index = pd.to_datetime(weather.index).tz_localize('UTC')
        if self.prefix:
            weather.columns = [f"{self.prefix}_{col}" for col in weather.columns]


        self.progress.update(task)
        self.progress.console.log("[green]Open Meteo downloaded successfully")
        self.progress.stop()
        return weather 
            