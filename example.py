from src.data.DataPipeline import DataPipeline
from src.data.sources.OpenMeteoSource import OpenMeteoSource
from src.data.sources.EntsoeSource import EntsoeSource
from src.data.transformations.CalendarTransformation import CalendarTransformation
from src.data.transformations.TimeZoneTransformation import TimeZoneTransformation
from src.data.transformations.LagTransformation import LagTransformation
from src.models.OLSModel import OLSModel
from src.models.NaiveModel import NaiveModel
from src.models.LassoModel import LassoModel
from src.evaluators.MaeEvaluator import MaeEvaluator
from src.data.DataPipeline import DataPipeline
from src.evaluators.EvaluatorPipeline import EvaluatorPipeline
import os
# os.environ["MAX_THREADS"] = 128 #by default all cores in the system

if __name__ == "__main__":
    pipeline = DataPipeline("2023-01-01", "2025-08-20")

    pipeline.add_source(EntsoeSource("PL", "API_KEY"))
    pipeline.add_source(OpenMeteoSource(52.2298, 21.0118, columns=["temperature_2m", "precipitation", "cloud_cover"], horizon=7))

    pipeline.add_transformation(TimeZoneTransformation(timezone="Europe/Warsaw"))

    pipeline.add_transformation(CalendarTransformation("PL"))
    pipeline.add_transformation(LagTransformation(columns=["load"], lags=range(0, 8), type="day"))
    pipeline.add_transformation(LagTransformation(columns=CalendarTransformation.weekly_dummies, lags=[-1, -2, -3, -4, -5, -6, -7], type="day"))

    data = pipeline.execute("example_dataset")  # if you don't want cache just remove the cache key or remove the example_cache.csv file

    pipeline = EvaluatorPipeline(data, "2024-08-01", "2025-08-01", horizon=7)
    
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
    
    pipeline.add_model(NaiveModel(name="Naive", target="load", saveToFile="naive_results.json"))
    pipeline.add_model(OLSModel(predictorsAR, name="AR", target="load", trainingWindow=364))
    pipeline.add_model(LassoModel(predictorsARX, modelParams={"max_iter": 10000}, name="ARX LASSO", target="load", trainingWindow=365))

    pipeline.add_evaluator(MaeEvaluator(type="hourly", name="Hourly MAE Evaluator"))
    pipeline.add_evaluator(MaeEvaluator(type="daily", name="Daily MAE Evaluator"))
    pipeline.add_evaluator(MaeEvaluator(type="all", name="MAE Evaluator"))

    pipeline.execute("example_results.xlsx")
