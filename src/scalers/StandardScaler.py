from sklearn.preprocessing import StandardScaler as StandardScalerSklearn
class StandardScaler:
    def __init__(self):
        pass
    
    def transform(self, train, test, predictors,target):
        scaler = StandardScalerSklearn()
        train[predictors] = scaler.fit_transform(train[predictors])
        test[predictors] = scaler.transform(test[predictors])
        
        self.scaler = StandardScalerSklearn()
        train.loc[:, target] = self.scaler.fit_transform(train[[target]])
        return test, train

    def inverse(self, prediction):
        return self.scaler.inverse_transform(prediction.reshape(-1, 1)).item()