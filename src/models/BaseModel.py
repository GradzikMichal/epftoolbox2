import os
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
import json
from .ModelWorker import ModelWorker
from abc import ABC, abstractmethod
from rich import print
from rich.progress import track
from multiprocessing import shared_memory
import multiprocessing as mp
import pandas as pd
import numpy as np
from ..scalers.StandardScaler import StandardScaler


class BaseModel(ABC):
    def __init__(
        self,
        predictors=[],
        name="Model Name",
        trainingWindow=28,
        modelParams={},
        internalParams={},
        saveToFile=None,
    ):
        self.predictors = predictors
        self.trainingWindow = trainingWindow
        self.modelParams = modelParams
        self.internalParams = internalParams
        self.saveToFile = saveToFile
        self.name = name
        self.scaler = StandardScaler()

    def preprocess(self, data, horizon, target):
        data = data.copy()
        for i in range(1, horizon + 1):
            data[f"{target}_d+{i}"] = data[target].shift(-24 * i)
        data["numeric_index"] = np.arange(1, len(data) + 1)
        data.index_col = pd.to_datetime(data.index_col)
        data["hour"] = data.index_col.hour
        data[f"ones"] = 1
        data["day"] = data["numeric_index"] // 24
        return data


    def beforeHook(self, horizon, data, testPeriodStart, testPeriodEnd, datasetOffset, testingWindow, target="load"):
        pass

    def run(self, horizon, data, testPeriodStart, testPeriodEnd, target="load"):
        if self.saveToFile and os.path.exists(f"./results/{self.saveToFile}"):
            os.makedirs("./results", exist_ok=True)
            print(f"[yellow]Loading model results from file {self.saveToFile}")
            with open(f"./results/{self.saveToFile}", "r") as f:
                return json.load(f)

        data = self.preprocess(data, horizon, target)
        datasetOffset = int(data.loc[testPeriodStart, "day"].values[0])
        testingWindow = int(data.loc[testPeriodEnd, "day"].values[0])

        data = data.drop('utc_datetime', axis=1, errors='ignore')
        shape = data.shape
        dtype = data.values.dtype
        shm = shared_memory.SharedMemory(create=True, size=data.values.nbytes)
        np_array = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
        np_array[:] = data.values[:]

        tasks = []
        results = []

        self.beforeHook(horizon, data, testPeriodStart, testPeriodEnd, datasetOffset, testingWindow, target)
     
        for dayInTestingPeriod in range(testingWindow - datasetOffset + 1):
            for currentHorizon in range(1, horizon + 1):
                for hour in range(0, 24):
                    context = {
                        "hour": hour,
                        "dayInTestingPeriod": dayInTestingPeriod,
                        "datasetOffset": datasetOffset,
                        "horizon": currentHorizon,
                        "trainingWindow": self.trainingWindow,
                        "modelParams": self.modelParams,
                        "internalParams": self.internalParams
                    }
                    context["target"] = f"{target}_d+{currentHorizon}"
                    context["predictors"] = self.processColumns(
                        self.predictors, context)
                    inMemoryData = {
                        "pointer": shm.name,
                        "shape": shape,
                        "dtype": dtype,
                        "index_col": data.index_col,
                        "columns": data.columns,
                    }
                    tasks.append(
                        (context, inMemoryData, self.one, self.scaler))
                    # results.append(ModelWorker.worker((context, inMemoryData, self.one, self.scaler)))

        processes = int(os.environ.get("MAX_THREADS") or mp.cpu_count())
        with mp.Pool(processes=processes) as pool:
            for result in track(
                pool.imap(ModelWorker.worker, tasks),
                description=f"[magenta]Running model {self.name}",
                total=len(tasks),
            ):
                results.append(result)

        shm.close()
        shm.unlink()

        if self.saveToFile:
            with open(f"./results/{self.saveToFile}", "w") as f:
                json.dump(results, f)

        return results

    def processColumns(self, columns, context):
        processedColumns = []
        if callable(columns):
            columns = columns(context)

        for column in columns:
            if callable(column):
                processedColumns.append(column(context))
            else:
                for key in context:
                    column = column.replace(f"{{{key}}}", str(context[key]))
                processedColumns.append(column)

        return processedColumns

    @staticmethod
    @abstractmethod
    def one(test, train, dayInTestingPeriod, currentHorizon, hour):
        pass
