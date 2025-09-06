from sklearn.neural_network import MLPRegressor
from .BaseModel import BaseModel

class MLPModel(BaseModel):
    @staticmethod
    def one(trainX, trainY, testX, **context):
        predictions = []
        for i in range(1):
            model = MLPRegressor(**context['modelParams'])
            model.fit(trainX, trainY.values.ravel())
            prediction = model.predict(testX)
            predictions.append(prediction)
        return sum(predictions) / len(predictions), []
