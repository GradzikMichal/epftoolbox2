import pandas as pd
import holidays as holidaysLib
from typing import Optional
from rich.progress import Progress

class CalendarSource():

    def __init__(self, country_code: str, weekly_dummies: Optional[str] = 'one-hot', holidays: Optional[str] = 'exists'):

        self.country_code = country_code
        self.weekly_dummies = weekly_dummies
        self.holidays = holidays
        self.progress = Progress()
        
        if self.holidays is not None:
            self.country_holidays = holidaysLib.country_holidays(self.country_code)

    weekly_dummies = ['is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday','is_holiday']

    def fetch(self, start_date, end_date) -> pd.DataFrame:
        self.progress.start()
        task = self.progress.add_task("[yellow]Calendar Source", total=2)

        dates = pd.date_range(start=start_date, end=end_date, freq='h', tz="UTC")
        calendar = pd.DataFrame({'date': dates})

        if self.weekly_dummies is not None:
            self.progress.console.log("[dim]Processing weekly dummies")
            if self.weekly_dummies == 'one-hot':
                days = ['is_monday', 'is_tuesday', 'is_wednesday', 'is_thursday', 'is_friday', 'is_saturday', 'is_sunday']
                for i, day_name in enumerate(days):
                    calendar[day_name] = (calendar['date'].dt.dayofweek == i).astype(int)
            elif self.weekly_dummies == 'list':
                calendar['weekday'] = calendar['date'].dt.day_name()
            elif self.weekly_dummies == 'number':
                calendar['weekday'] = calendar['date'].dt.dayofweek + 1
                
        self.progress.update(task,advance=1)

        if self.holidays is not None:
            self.progress.console.log("[dim]Processing holidays")
            holiday_names = calendar['date'].apply(lambda x: self.country_holidays.get(x))

            if self.holidays == 'exists':
                calendar['is_holiday'] = (holiday_names.notna()).astype(int)
            elif self.holidays == 'one-hot':
                unique_holidays = holiday_names.dropna().unique()
                for holiday in unique_holidays:
                    calendar[f"is_{holiday.lower().replace(' ','_')}"] = (holiday_names == holiday).astype(int)
            elif self.holidays == 'list':
                calendar['holiday_name'] = holiday_names.fillna('')

        calendar.set_index('date', inplace=True)
        calendar.index.name="datetime"
        self.progress.update(task,advance=1)
        self.progress.console.log("[green]Calendar data added successfully")
        self.progress.stop()

        return calendar