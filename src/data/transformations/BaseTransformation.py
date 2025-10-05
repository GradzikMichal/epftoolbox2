import pandas as pd


class BaseTransformation:

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
