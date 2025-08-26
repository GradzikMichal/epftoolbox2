# epf-toolbox-2
Open source toolbox for energy market data and forecasting. Very much in progress. Based on idea of https://github.com/jeslago/epftoolbox

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
from src.data.sources.OpenMeteoSource import OpenMeteoSource
openmeteo_source = OpenMeteoSource(52.2298,21.0118)
data = openmeteo_source.fetch("2023-01-01", "2025-01-07")
```
As output by default you get columns: datetime,temperature_2m,rain,showers,snowfall,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,weather_code,surface_pressure,pressure_msl,cloud_cover,wind_speed_10m,wind_direction_10m along with 7 day ahead forecasts of this columns in `_d+{horizon}` surfix, so for example rain_d+2.
In params you gen specify the horizon (7 days by default), used model (JMA by default) and columns. 

## Transformations
### TimeZoneTransformation
```python
from src.data.transformations.TimeZoneTransformation import TimeZoneTransformation

timezone_transformation = TimeZoneTransformation("Europe/Warsaw", gmt_column=True)
data = timezone_transformation.transform(data)
```
This transformation enables the data confersion from default UTC to local timezone. It accepts 3 params:
- timezone
- interpolation - we want to keep the 24 hour per day format, so we interpolate the outliers coused by DTS. You can set it to "linear", "first" or None
- gmt_column - enables to attach the gtm_offset column to the dataset with numerical values

The output column will have prefixes like _d+1 or _h+12.
### LagTransformation
```python
from src.data.transformations.LagTransformation import LagTransformation

lag_transformation = LagTransformation(columns=["load","price"], lags=[1,3,7], type="day")
data = lag_transformation.transform(data)
```
The base class accepts 3 params:
- columns
- lags - you can pass array of lags, range object or negative value
- type - day or hour

The output column will have prefixes like _d+1 or _h+12.

### Calendar Transformation
```python
from src.data.transformations.CalendarTransformation import CalendarTransformation

calendar_source = CalendarSource("PL")
data = calendar_source.transform(data)
```
In the base class you can define two params. `weekly_dummies` accepts values:
- one-hot -> 'is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday' columns are created with values 0 or 1
- list -> the full name in english is given
- number -> number from 1 do 7 is given

Make sure to execute it after the TimeZoneTransformation so your dummies are adjusted to the local tz.

## Models
### Naive Model
Naive model takes the previous value from 7 days ago relative to the target date.
```python
from src.models.NaiveModel import NaiveModel
model = NaiveModel(name="Naive", saveToFile="naive_results.json")
results = model.run(horizon = 7, data,  "2024-08-01", "2025-08-01", target="load")
```
### OLS Model
```python
from src.models.OLSModel import OLSModel
model = OLSModel(
    ["load", "is_holiday_d+{horizon}", lambda ctx: f"load_d-{7-ctx['horizon']}"],
    trainingWindow=364,
    saveToFile="naive_results.json"
     )
results = model.run(horizon = 7, data,  "2024-08-01", "2025-08-01", target="load")
```
As predictors you can pass:
- just columns in array from the dataset
- column with one of tags: {horizon}, {hour},{trainingWindow}, {dayInTestingPeriod}, {datasetOffset}
- lambda function with ctx containig tags as keys along "modelsParams" object. It enables you to conduct additional operations
- predefined function - analog to previous lambda
- predefined function as whole predictors object - it should return the whole predictors array. The most elastic options

### Lasso Model
```python
from src.models.LassoModel import LassoModel
model = LassoModel(
    ["load", "is_holiday_d+{horizon}", lambda ctx: f"load_d-{7-ctx['horizon']}"],
    trainingWindow=364,
    modelParams={"max_iter": 10000},
    name="LASSO",
    )
results = model.run(horizon = 7, data,  "2024-08-01", "2025-08-01", target="load")
```

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
Evaluator let's you compare multiple models. Define the test period, then the models itself and choose evaluation metrics. As the result you will recive excel file with all the comparasions.
```python
    pipeline = EvaluatorPipeline(data, "2024-08-01", "2025-08-01", horizon=7, target="load")
    
    predictorsAR =  [
        "load",
        "load_d-1",
        lambda ctx: f"load_d-{7-ctx['horizon']}",
        "is_monday_d+{horizon}",
        "is_tuesday_d+{horizon}",
        "is_wednesday_d+{horizon}",
        "is_thursday_d+{horizon}",
        "is_friday_d+{horizon}",
        "is_saturday_d+{horizon}",
        "is_sunday_d+{horizon}",
        "is_holiday_d+{horizon}",
    ]
    
    predictorsARX = [
        *predictorsAR,
        "temperature_2m_d+{horizon}",
        "cloud_cover_d+{horizon}",
        "precipitation_d+{horizon}",
    ]
    
    pipeline.add_model(NaiveModel(name="Naive", saveToFile="naive_results.json"))
    pipeline.add_model(OLSModel(predictorsAR, name="AR", trainingWindow=364, saveToFile="ar_results.json"))
    pipeline.add_model(LassoModel(predictorsARX, modelParams={"max_iter": 10000}, name="ARX LASSO", trainingWindow=365,saveToFile="arx_results.json")))

    pipeline.add_evaluator(MaeEvaluator(type="hourly", name="Hourly MAE Evaluator"))
    pipeline.add_evaluator(MaeEvaluator(type="daily", name="Daily MAE Evaluator"))
    pipeline.add_evaluator(MaeEvaluator(type="all", name="MAE Evaluator"))

    pipeline.execute("example_results.xlsx")
```

The results are cached with saveToFile key. If you want to run the models every time for scrach use `pipeline.clear_cache()` function.
Start and End testing period are the dates of the first and last steps, so you need to keep in mind that with horizon 7 you also need to have the target data in dataset. 