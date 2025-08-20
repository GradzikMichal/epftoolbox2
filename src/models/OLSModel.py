from sklearn import linear_model
from .BaseModel import BaseModel


class OLSModel(BaseModel):
    def one(self, trainX, trainY, testX):
        model = linear_model.LinearRegression(**{"fit_intercept": False, **self.modelParams})
        model.fit(trainX, trainY)
        prediction = model.predict(testX)
        prediction = self.scaler.inverse(prediction)
        return prediction, model.coef_
      
