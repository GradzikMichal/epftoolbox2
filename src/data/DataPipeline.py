import pandas as pd

class DataPipeline:
    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date
        self.sources = []
        self.transformations = []

    def add_source(self, source):
        self.sources.append(source)
    
    def add_transformation(self, transformation):
        self.transformations.append(transformation)
        
    def execute(self, cache = False) -> pd.DataFrame:
        if cache and pd.io.common.file_exists(f'{cache if (type(cache) is str) else "cache"}.csv'):
            return pd.read_csv(f'{cache if (type(cache) is str) else "cache"}.csv', index_col=0)

        merged = pd.DataFrame()
        for source in self.sources:
            df = source.fetch(self.start_date, self.end_date)
            merged = pd.merge(merged, df, left_index=True, right_index=True, how='outer')
        
        for transformation in self.transformations:
            merged = transformation.transform(merged)
        
        if cache:
            merged.to_csv(f'{cache if (type(cache) is str) else "cache"}.csv')
            
        return merged