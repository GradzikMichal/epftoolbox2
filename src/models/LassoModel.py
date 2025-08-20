from sklearn import linear_model

from .BaseModel import BaseModel


class LassoModel(BaseModel):
    def one(self, trainX, trainY, testX):
        model = linear_model.LassoCV(*self.modelParams)
        model.fit(trainX, trainY.values.ravel())
        prediction = model.predict(testX)
        prediction = self.scaler.inverse(prediction)
        return prediction, model.coef_

