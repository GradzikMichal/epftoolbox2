import numpy as np
from sklearn.neural_network import MLPRegressor
from .BaseModel import BaseModel
from sklearn.experimental import enable_halving_search_cv
from sklearn.model_selection import HalvingRandomSearchCV
from scipy.stats import uniform
from sklearn.base import BaseEstimator, RegressorMixin
from .ModelWorker import ModelWorker
from functools import partial
import optuna
#Wrapper for MLP regressor to implement committee of models
class MLPCommittee(BaseEstimator, RegressorMixin):
    def __init__(self, n_members=5, **mlp_params):
        self.n_members = n_members
        self.mlp_params = mlp_params
        self.models_ = []

    def fit(self, X, y):
        self.models_ = []
        for _ in range(self.n_members):
            model = MLPRegressor(**self.mlp_params)
            model.fit(X, y)
            self.models_.append(model)
        return self

    def predict(self, X):
        if not self.models_:
            raise Exception("The model has not been fitted yet.")
            
        predictions = [model.predict(X) for model in self.models_]
        return np.mean(predictions, axis=0)

    def get_params(self, deep=True):
        params = super().get_params(deep)
        params.update(self.mlp_params)
        return params

    def set_params(self, **params):
        mlp_param_keys = MLPRegressor().get_params().keys()
        
        self_params = {}
        child_params = {}
        
        for key, value in params.items():
            if key in mlp_param_keys:
                child_params[key] = value
            else:
                self_params[key] = value

        self.mlp_params.update(child_params)
        return super().set_params(**self_params)

class MLPModel(BaseModel):
    def __init__( self,
        predictors=[],
        name="Model Name",
        trainingWindow=28,
        modelParams={},
        committee=5,
        hyperparamOptimization=False, #cv, number meaning calibration window
        cvCount=5,
        saveToFile=None,
    ):
        super().__init__(
            predictors=predictors,
            name=name,
            trainingWindow=trainingWindow,
            modelParams=modelParams,
            saveToFile=saveToFile,
            internalParams={'committee': committee, 'hyperparamOptimization': hyperparamOptimization, 'cvCount': cvCount}
        )

    def beforeHook(self, horizon, data, testPeriodStart, testPeriodEnd, datasetOffset, testingWindow, target="load"):
     
        if type(self.internalParams.get('hyperparamOptimization'))==int:
            study = optuna.create_study()
            objective = partial(MLPModel.optimizationObjective,
                                horizon=horizon,
                                data=data,
                                testPeriodStart=data[data['day'] == datasetOffset-self.internalParams.get('hyperparamOptimization')].index_col[0].strftime('%Y-%m-%d'),
                                testPeriodEnd=testPeriodStart,
                                target=target,
                                predictors=self.predictors,
                                trainingWindow=self.trainingWindow,
                                modelParams=self.modelParams,
                                name=self.name
                                )
            self.internalParams['hyperparamOptimization']=False
            study.optimize(objective, n_trials=1,n_jobs=1)
            self.modelParams = study.best_params
            print(f"[green]Optimization complete. Best params: {study.best_params}")
        
    @staticmethod
    def optimizationObjective(trial, horizon, data, testPeriodStart, testPeriodEnd, target, predictors, trainingWindow, modelParams,name):
        modelParams['hidden_layer_sizes'] = modelParams.get('hidden_layer_sizes') or trial.suggest_categorical('hidden_layer_sizes', [(5,), (10,), (5,5), (10,5), (10,10)])
        modelParams['solver'] = modelParams.get('solver') or trial.suggest_categorical('solver', ['adam', 'sgd', 'lbfgs'])
        modelParams['activation'] = modelParams.get('activation') or trial.suggest_categorical('activation', ['relu', 'tanh', 'logistic'])
        modelParams['alpha'] = modelParams.get('alpha') or trial.suggest_float('alpha', 0.0001, 0.01)
        if modelParams['solver'] == 'lbfgs':
            modelParams['max_iter'] = modelParams.get('max_iter') or trial.suggest_categorical('max_iter', [500, 1000, 5000, 10000, 20000])
        else:   
            modelParams['learning_rate_init'] = modelParams.get('learning_rate_init') or trial.suggest_float('learning_rate_init', 0.001, 0.1)
        model = MLPModel(predictors=predictors, trainingWindow=trainingWindow, name=f"{name} trial #{trial.number}", modelParams=modelParams,hyperparamOptimization=False)
        results = model.run(horizon, data, testPeriodStart, testPeriodEnd, target)
        errors = [abs(res['value'] - res['prediction']) for res in results]
        return np.mean(errors)

            


    @staticmethod
    def one(trainX, trainY, testX, **context):
        
        initial_params = context.get('modelParams', {}).copy()
        initial_params['n_members'] = context['internalParams'].get('committee', 5)
        
        model = MLPCommittee(**initial_params)

        if context['internalParams'].get('hyperparamOptimization', False)=='cv':
            params = [
                {
                    'solver': ['adam', 'sgd'],
                    'activation': ['relu', 'tanh', 'logistic'],
                    'alpha': uniform(0.0001, 0.01),
                    'learning_rate_init': uniform(0.001, 0.1),
                },
                {
                    'solver': ['lbfgs'],
                    'activation': ['relu', 'tanh', 'logistic'],
                    'alpha': uniform(0.0001, 0.01),
                    'max_iter': [500, 1000, 5000, 10000, 20000],
                }
            ]

            search = HalvingRandomSearchCV(
                estimator=model,
                param_distributions=params,
                n_candidates='exhaust',
                factor=3,
                cv=context['internalParams'].get('cvCount', 5),
                n_jobs=1
            )

            search.fit(trainX, trainY.values.ravel())
            best = search.best_estimator_
            prediction = best.predict(testX)
            return prediction, best.get_params()  
        else:
            model.fit(trainX, trainY.values.ravel())
            prediction = model.predict(testX)


        
        return prediction, model.get_params()