from ..scalers.StandardScaler import StandardScaler
import numpy as np
import pandas as pd
import multiprocessing
from rich.progress import track
from rich import print
import os
from abc import ABC,abstractmethod
import json
os.environ["MKL_NUM_THREADS"] = "1" 
os.environ["NUMEXPR_NUM_THREADS"] = "1" 
os.environ["OMP_NUM_THREADS"] = "1" 
def worker(args):
    model, test, train, dayInTestingPeriod, currentHorizon, hour, scalablePredictors, otherPredictors = args
    predictors = [*scalablePredictors, *otherPredictors]
    targetColumn = f"{model.target}_d+{currentHorizon}"
    test, train = model.scaler.transform(train,test,scalablePredictors,targetColumn)
    trainX = train[predictors]
    trainY = train[[targetColumn]]
    testX = test[predictors]
    prediction, params = model.one(trainX, trainY, testX)
    return  {
        "date": str(np.datetime_as_string(test.index.values[0], unit='D')),
        "hour": hour,
        "horizon": currentHorizon,
        'dayInTestingPeriod':dayInTestingPeriod,
        "prediction": prediction,
        "value": float(test[targetColumn].iloc[0]),
        "coefs": params.tolist(),
        "predictors": predictors
    }
class BaseModel(ABC):
    def __init__(self,scalablePredictors=[], otherPredictors=[],target='load',name="Model Name",trainingWindow=28,modelParams={}, cpuCores=multiprocessing.cpu_count(), saveToFile=None):
        self.scalablePredictors = scalablePredictors
        self.otherPredictors = otherPredictors
        self.target = target
        self.cpuCores = cpuCores
        self.trainingWindow = trainingWindow
        self.modelParams = modelParams
        self.saveToFile = saveToFile
        self.name = name
        self.scaler = StandardScaler()

    weekly_dummies = ['is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday','is_holiday']
    
    def preprocess(self,data, horizon):
        data = data.copy()
        for i in range(1, horizon+1):
            data[f'{self.target}_d+{i}']=data[self.target].shift(-24*i)
        data['numeric_index'] = np.arange(1, len(data) + 1)
        data.index=pd.to_datetime(data.index)
        data["hour"] = data.index.hour
        data[f"ones"] = 1
        data["day"] = data['numeric_index'] // 24
        return data
    
    def run(self,horizon,data,testPeriodStart,testPeriodEnd):
            data = self.preprocess(data, horizon)
            
            datasetOffset = data.loc[testPeriodStart, 'day'].values[0]
            testingWindow = data.loc[testPeriodEnd, 'day'].values[0]
            
            tasks=[]
            for dayInTestingPeriod in track(range(testingWindow-datasetOffset+1-horizon),description=f"[magenta]Preparing data for {self.name}"):
                for currentHorizon in range(1,horizon+1):
                    for hour in range(0,24):
                        test, train = self.extractTrainAndTest(data,hour,dayInTestingPeriod,datasetOffset,horizon)
                        tasks.append((
                            self,
                            test,
                            train,
                            dayInTestingPeriod,
                            currentHorizon,
                            hour,
                            self.processPredictors(self.scalablePredictors, currentHorizon),
                            self.processPredictors(self.otherPredictors, currentHorizon)))
            
            with multiprocessing.Pool(processes=self.cpuCores) as pool:
                    results = list(track(
                        pool.imap(worker, tasks),
                        description=f"[magenta]Running model {self.name}",
                        total=len(tasks)
                    ))
            
            if self.saveToFile:
                with open(f"./results/{self.saveToFile}", "w") as f:
                    json.dump(results, f)

            return results
    
    def processPredictors(self,predictors,horizon=0):
        run={'horizon': horizon}
        processedPredictors = []
        if callable(predictors):
            predictors = predictors(run)
        for predictor in predictors:
            if callable(predictor):
                processedPredictors.append(predictor(run))
            else:
                processedPredictors.append(predictor.replace("{horizon}", str(horizon)))
        
        return processedPredictors

    
    @abstractmethod
    def one(self, test, train,dayInTestingPeriod, currentHorizon, hour):
        pass        
        
    def extractTrainAndTest(self, data,hour,dayInTestingPeriod,datasetOffset, horizon):
         filteredData = data[
                    (data["hour"] == hour)
                    & (data["day"] >= datasetOffset - self.trainingWindow - horizon + dayInTestingPeriod) # we cannot use future data for training in case of longer horizons (or in case of 9 am cutoff), co we move the trainig period few days back
                    & (data["day"] < datasetOffset + dayInTestingPeriod + 1)
                ]
         train = filteredData.head(-1-horizon) # we cannot use future data for training in case of longer horizons (or in case of 9 am cutoff), co we move the trainig period few days back
         test = filteredData.tail(1)
         return test, train