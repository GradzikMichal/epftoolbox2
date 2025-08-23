from ..scalers.StandardScaler import StandardScaler
import numpy as np
from multiprocessing import shared_memory
import pandas as pd

class ModelWorker:
    @staticmethod
    def worker(args):
        context, inMemoryData, model, scaler = args
        data, sharedMemory = ModelWorker.getDataFromSharedMemory(inMemoryData)
        test, train = ModelWorker.extractTrainAndTest(data, context["hour"], context["dayInTestingPeriod"],  context["datasetOffset"], context['horizon'], context['trainingWindow'])
        test, train = scaler.transform(train,test,context['predictors'], context['target'])
        trainX = train[context['predictors']]
        trainY = train[[context['target']]]
        testX = test[context['predictors']]
        
        prediction, params = model(trainX, trainY, testX, **context)
        prediction = scaler.inverse(prediction)
        sharedMemory.close()
        return  {
            "date": str(np.datetime_as_string(test.index.values[0], unit='D')),
            **context,
            "prediction": prediction,
            "value": float(test[context['target']].iloc[0]),
            "testX": testX.iloc[0].values.tolist(),
            "coefs": params,
        }
        
    @staticmethod
    def getDataFromSharedMemory(inMemoryData):
        sharedMemory = shared_memory.SharedMemory(name=inMemoryData['pointer'])
        sharedArray = np.ndarray(inMemoryData['shape'], dtype=inMemoryData['dtype'], buffer=sharedMemory.buf)
        data = pd.DataFrame(sharedArray, index=inMemoryData['index'], columns=inMemoryData['columns'])
        return data, sharedMemory
        
    
    @staticmethod
    def extractTrainAndTest(
        data, hour, dayInTestingPeriod, datasetOffset, horizon, trainingWindow
    ):
        filteredData = data[
            (data["hour"] == hour)
            & (
                data["day"]
                >= datasetOffset - trainingWindow - horizon + dayInTestingPeriod
            )  # we cannot use future data for training in case of longer horizons (or in case of 9 am cutoff), co we move the trainig period few days back
            & (data["day"] < datasetOffset + dayInTestingPeriod + 1)
        ]
        train = filteredData.head(
            -1 - horizon
        )  # we cannot use future data for training in case of longer horizons (or in case of 9 am cutoff), co we move the trainig period few days back
        test = filteredData.tail(1)
        
        return test, train
 