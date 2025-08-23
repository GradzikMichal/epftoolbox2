from sklearn.preprocessing import StandardScaler as StandardScalerSklearn
class StandardScaler:
    def __init__(self):
        pass
    
    def transform(self, train, test, predictors,target):
        dummy_columns = [col for col in predictors if train[col].nunique() == 2]
        numeric_columns = [col for col in predictors if (col not in dummy_columns) and (col != target)]
        scaler = StandardScalerSklearn()
        train[numeric_columns] = scaler.fit_transform(train[numeric_columns])
        test[numeric_columns] = scaler.transform(test[numeric_columns])

        self.scaler = StandardScalerSklearn()
        train.loc[:, target] = self.scaler.fit_transform(train[[target]])
        return test, train

    def inverse(self, prediction):
        return self.scaler.inverse_transform(prediction.reshape(-1, 1)).item()