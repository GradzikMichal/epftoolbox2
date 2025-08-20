from src.data.DataPipeline import DataPipeline
from src.data.sources.OpenMeteoSource import OpenMeteoSource
from src.data.sources.EntsoeSource import EntsoeSource
from src.data.sources.CalendarSource import CalendarSource
from src.data.transformations.TimeZoneTransformation import TimeZoneTransformation
from src.data.transformations.LagTransformation import LagTransformation
from src.models.OLSModel import OLSModel
from src.models.LassoModel import LassoModel
from src.evaluators.MaeEvaluator import MaeEvaluator
from src.data.DataPipeline import DataPipeline
from src.evaluators.EvaluatorPipeline import EvaluatorPipeline

if __name__ == "__main__":
    pipeline = DataPipeline("2023-01-01","2025-08-20")

    pipeline.add_source(EntsoeSource("PL", API_KEY))
    pipeline.add_source(OpenMeteoSource(52.2298,21.0118, columns=["temperature_2m", "precipitation","cloud_cover"], horizon=1))
    pipeline.add_source(CalendarSource("PL"))
    
    pipeline.add_transformation(TimeZoneTransformation(timezone="Europe/Warsaw"))
    
    pipeline.add_transformation(LagTransformation(columns=["load"], lags=[1,7], type='day'))
    
    data=pipeline.execute("example_dataset") #if you don't want cache just remove the cache key or remove the example_cache.csv file
    
    dummies = ['is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday','is_holiday']
    
    pipeline = EvaluatorPipeline(1, data, "2024-08-01", "2025-08-01")
    
    pipeline.add_model(OLSModel(['load','load_d-1','load_d-7'], dummies, name="AR", target='load', trainingWindow=365, saveToFile="ar.json"))
    pipeline.add_model(LassoModel(
        ['load','load_d-1','load_d-7','temperature_2m_d+1','cloud_cover_d+1','precipitation_d+1'], dummies,
        modelParams={"max_iter": 10000},
        name="ARX",
        target='load',
        trainingWindow=365,
        saveToFile="arx.json"
    ))

    pipeline.add_evaluator(MaeEvaluator(type='hourly', name="Hourly MAE Evaluator"))
    pipeline.add_evaluator(MaeEvaluator(type='all', name="MAE Evaluator"))
    
    pipeline.execute("example_results.xlsx")

    
 