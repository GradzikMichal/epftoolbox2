from sklearn import linear_model
from .BaseModel import BaseModel
import matplotlib.pyplot as plt

def basicWeightFunction(context,trainX,testX):
    return [1]*len(trainX)

class WLSModel(BaseModel):
    def __init__( self,
        predictors=[],
        name="Model Name",
        trainingWindow=28,
        modelParams={},
        weightFunction=basicWeightFunction,
        saveToFile=None,
    ):
        super().__init__(
            predictors=predictors,
            name=name,
            trainingWindow=trainingWindow,
            modelParams=modelParams,
            saveToFile=saveToFile,
            internalParams={'weightFunction': weightFunction}
        )
        

    @staticmethod
    def one(trainX, trainY, testX, **context):
        model = linear_model.LinearRegression(**{"fit_intercept": False, **context['modelParams']})
        weights = context['internalParams'].get('weightFunction')(context,trainX,testX)
        model.fit(trainX, trainY,sample_weight=weights)
        prediction = model.predict(testX)
        return prediction, model.coef_.tolist()[0]
      
