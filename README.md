# epf-toolbox-2
Open source toolbox for energy market data and forecasting. Very much in progress.

# Overall concept
You have building blocks for sources, transformations, models and evaluators. You can use them in standalone mode to play with the individual aspects or you can use pipelines to combine multiple sources transformations, models and evaluators to achive standarized workflow. 

# Docs
## Instalation
### UV way
Install [uv](https://github.com/astral-sh/uv) in your system and run
```python
uv run example.py
```

### PIP way
Run `pip install -r requirements.txt` and then `python example.py`
## Sources

### Entsoe Source
```python
from src.data.sources.EntsoeSource import EntsoeSource
entsoe_source = EntsoeSource(api_key=API_KEY, country_code="PL")
data = entsoe_source.fetch("2023-01-01", "2025-01-07")
```
As output you get columns: datetime,load,price. You can get prices as far as now+1, becouse of the day ahead prices mechanics of the energy markets.
### OpenMeteo Source
```python
from src.data.sources.EntsoeSource import OpenMeteoSource
openmeteo_source = OpenMeteoSource(52.2298,21.0118)
data = openmeteo_source.fetch("2023-01-01", "2025-01-07")
```
As output by default you get columns: datetime,temperature_2m,rain,showers,snowfall,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,weather_code,surface_pressure,pressure_msl,cloud_cover,wind_speed_10m,wind_direction_10m along with 7 day ahead forecasts of this columns in `_d+{horizon}` surfix, so for example rain_d+2.
In params you gen specify the horizon (7 days by default), used model (JMA by default) and columns. 
### Calendar Source
```python
from src.data.sources.EntsoeSource import CalendarSource
calendar_source = CalendarSource("PL")
data = calendar_source.fetch("2023-01-01", "2025-01-07")
```
In the base class you can define two params. `weekly_dummies` accepts values:
- one-hot -> 'is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday' columns are created with values 0 or 1
- list -> the full name in english is given
- number -> number from 1 do 7 is given

## Transformations
### TimeZoneTransformation
### LagTransformation

## Models
### OLS Model
### LassoModes

## Evaluators
### Mae Evaluator

## Pipelines 
### Data Pipeline
Data pipeline enables us to define multiple sources and combine multiple datatables.
```python
    from src.data.DataPipeline import DataPipeline
    from src.data.sources.OpenMeteoSource import OpenMeteoSource
    from src.data.sources.EntsoeSource import EntsoeSource
    from src.data.sources.CalendarSource import CalendarSource
    from src.data.transformations.TimeZoneTransformation import TimeZoneTransformation
    from src.data.transformations.LagTransformation import LagTransformation

    pipeline = DataPipeline("2023-07-01","2025-08-20")

    pipeline.add_source(EntsoeSource("PL","fade2e5f-6d62-4354-9f95-e8629acec0e9"))
    pipeline.add_source(OpenMeteoSource(52.2298,21.0118))
    pipeline.add_source(CalendarSource("PL"))

    pipeline.add_transformation(TimeZoneTransformation(timezone="Europe/Warsaw"))
    pipeline.add_transformation(LagTransformation(columns=["load"], lags=[1,7], type='day'))

    data=pipeline.execute(cache="data_cache")
```
Cache param will save the data under this name in csv. In the next run the csv will be used as source.

### Evaluator Pipeline
