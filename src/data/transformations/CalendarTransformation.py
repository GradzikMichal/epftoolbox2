import pandas as pd
import holidays as holidaysLib
from typing import Optional
from rich.progress import Progress

class CalendarTransformation():

    def __init__(self, country_code: str, weekly_dummies: Optional[str] = 'one-hot', holidays: Optional[str] = 'exists'):

        self.country_code = country_code
        self.weekly_dummies = weekly_dummies
        self.holidays = holidays
        self.progress = Progress()
        
        if self.holidays is not None:
            self.country_holidays = holidaysLib.country_holidays(self.country_code)

    weekly_dummies = ['is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday','is_holiday']

    def transform(self, data) -> pd.DataFrame:
        self.progress.start()
        task = self.progress.add_task("[yellow]Calendar Source", total=2)

        if self.weekly_dummies is not None:
            self.progress.console.log("[dim]Processing weekly dummies")
            if self.weekly_dummies == 'one-hot':
                days = ['is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday']
                for i, day_name in enumerate(days):
                    data[day_name] = (data.index.dayofweek == i).astype(int)
            elif self.weekly_dummies == 'list':
                data['weekday'] = data.index.day_name()
            elif self.weekly_dummies == 'number':
                data['weekday'] = data.index.dayofweek + 1

        self.progress.update(task,advance=1)

        if self.holidays is not None:
            self.progress.console.log("[dim]Processing holidays")
            holiday_names = data.index.to_series().apply(lambda x: self.country_holidays.get(x))

            if self.holidays == 'exists':
                data['is_holiday'] = (holiday_names.notna()).astype(int)
            elif self.holidays == 'one-hot':
                unique_holidays = holiday_names.dropna().unique()
                for holiday in unique_holidays:
                    data[f"is_{holiday.lower().replace(' ','_')}"] = (holiday_names == holiday).astype(int)
            elif self.holidays == 'list':
                data['holiday_name'] = holiday_names.fillna('')

        # data.set_index('datetime', inplace=True)
        # data.index.name="datetime"
        self.progress.update(task,advance=1)
        self.progress.console.log("[green]Calendar data added successfully")
        self.progress.stop()

        return data