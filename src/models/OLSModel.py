from sklearn import linear_model
from .BaseModel import BaseModel

class OLSModel(BaseModel):
    @staticmethod
    def one(trainX, trainY, testX, **context):
        model = linear_model.LinearRegression(**{"fit_intercept": False, **context['modelParams']})
        model.fit(trainX, trainY)
        prediction = model.predict(testX)
        return prediction, model.coef_.tolist()
      
