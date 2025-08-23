from ..scalers.NoScaler import NoScaler
from .BaseModel import BaseModel


class NaiveModel(BaseModel):
    def __init__(self, target, predictors=[], name=None, saveToFile=None):
        super().__init__(
            predictors=predictors if predictors else [lambda context: f"{target}_d-{7 - context['horizon']}" if context['horizon'] < 7 else f"{target}"],
            target=target,
            name=name,
            trainingWindow=7,
            modelParams={},
            saveToFile=saveToFile
        )
        self.scaler = NoScaler()

    @staticmethod
    def one(trainX, trainY, testX, **context):
        prediction = testX.values[0][0]
        return prediction, []

