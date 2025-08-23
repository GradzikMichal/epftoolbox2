from sklearn import linear_model

from .BaseModel import BaseModel


class LassoModel(BaseModel):
    @staticmethod
    def one(trainX, trainY, testX, **context):
        model = linear_model.LassoCV(**context['modelParams'])
        model.fit(trainX, trainY.values.ravel())
        prediction = model.predict(testX)
        return prediction, model.coef_.tolist()

