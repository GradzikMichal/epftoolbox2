class NoScaler:
    def __init__(self):
        pass
    
    def transform(self, train, test, predictors,target):
        return test, train

    def inverse(self, prediction):
        return prediction