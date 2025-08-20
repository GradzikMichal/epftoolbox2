import pandas as pd
import numpy as np
from rich import print

class LagTransformation:

    def __init__(self, columns= [], lags=[], type='day'):
        self.columns = columns
        self.lags = lags
        self.type = type #day or hour

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        print(f"[dim]Transforming lags {self.columns}")
        new_columns = {}
        for column in self.columns:
            for lag in self.lags:
                lagged_column_name = f"{column}_{"d" if self.type == 'day' else 'h'}{'-' if lag >=0 else '+'}{abs(lag)}"
                shift_amount = lag * (24 if self.type == 'day' else 1)
                new_columns[lagged_column_name] = data[column].shift(shift_amount)
        
        lagged_df = pd.DataFrame(new_columns)
        result = pd.concat([data, lagged_df], axis=1)
        
        print(f"[green]Lag transformation successful")
        return result
