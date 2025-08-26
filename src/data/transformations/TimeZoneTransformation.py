import pandas as pd
import numpy as np
from rich import print

class TimeZoneTransformation:

    def __init__(self, timezone: str, interpolation: str = "linear", gmt_column: bool = True):
        self.timezone = timezone
        self.interpolation = interpolation
        self.gmt_column = gmt_column

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data['utc_datetime']=data.index
        print(f"[dim]Converting to timezone {self.timezone}")
        data = data.tz_convert(self.timezone)
        if self.gmt_column:
            data['gmt_offset'] = data.index.map(lambda x: int(x.utcoffset().total_seconds() / 3600))
            
        data.index = pd.to_datetime(data.index.strftime('%Y-%m-%d %H:%M'))
        if self.interpolation !=None:
            data = data.resample('h') 
        
        if self.interpolation=="linear":
            data = data.mean().interpolate(method='linear')
        elif self.interpolation=="first":
            data = data.first().ffill()
            
        data = data.round(2)
        print(f"[green]Timezone transformation successful")
        return data