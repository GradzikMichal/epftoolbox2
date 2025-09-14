import pandas as pd
import holidays as holidaysLib
from typing import Optional
from rich.progress import Progress

from src.data.transformations.BaseTransformation import BaseTransformation


class CalendarTransformation(BaseTransformation):

    def __init__(self, country_code: str, weekly_dummies: Optional[str] = 'one-hot', monthly_dummies: Optional[str] = None,quarterly_dummies: Optional[str] = None, holidays: Optional[str] = 'exists'):

        self.country_code = country_code
        self.weekly_dummies = weekly_dummies
        self.monthly_dummies = monthly_dummies
        self.quarterly_dummies = quarterly_dummies
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

        if self.monthly_dummies is not None:
            self.progress.console.log("[dim]Processing monthly dummies")
            if self.monthly_dummies == 'one-hot':
                months = ['is_january', 'is_february', 'is_march', 'is_april', 'is_may', 'is_june', 'is_july', 'is_august', 'is_september', 'is_october', 'is_november', 'is_december']
                for i, month_name in enumerate(months):
                    data[month_name] = (data.index.month == i + 1).astype(int)
            elif self.monthly_dummies == 'list':
                data['month'] = data.index.month_name()
            elif self.monthly_dummies == 'number':
                data['month'] = data.index.month

        if self.quarterly_dummies is not None:
            self.progress.console.log("[dim]Processing quarterly dummies")
            if self.quarterly_dummies == 'one-hot':
                quarters = ['is_q1', 'is_q2', 'is_q3', 'is_q4']
                for i, quarter_name in enumerate(quarters):
                    data[quarter_name] = (data.index.quarter == i + 1).astype(int)
            elif self.quarterly_dummies == 'list':
                data['quarter'] = data.index.quarter
            elif self.quarterly_dummies == 'number':
                data['quarter'] = data.index.quarter

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