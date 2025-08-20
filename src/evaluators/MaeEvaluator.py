import pandas as pd
import collections
from .BaseEvaluator import BaseEvaluator
from rich import print

class MaeEvaluator(BaseEvaluator):
    def __init__(self, type='daily', name='MAE Report'):
        super().__init__(name)
        self.type = type  # Can be 'daily', 'hourly', or 'all'

    def evaluate(self, data):
        if self.type == 'all':
            return self._evaluate_all(data)
        elif self.type == 'daily':
            return self._evaluate_daily(data)
        elif self.type == 'hourly':
            return self._evaluate_hourly(data)
        return {}

    def _calculate_mae(self, errors):
        return sum(errors) / len(errors) if errors else 0

    def _evaluate_all(self, data):
        errors = [abs(item['prediction'] - item['value']) for item in data if 'prediction' in item]
        return {'overall_mae': self._calculate_mae(errors)}

    def _evaluate_daily(self, data):
        daily_errors = collections.defaultdict(list)
        for item in data:
            daily_errors[f"horizon_{item['horizon']}"].append(abs(item['prediction'] - item['value']))
        
        return {horizon: self._calculate_mae(errors) for horizon, errors in daily_errors.items()}

    def _evaluate_hourly(self, data):
        hourly_errors = collections.defaultdict(lambda: collections.defaultdict(list))
        for item in data:
            error = abs(item['prediction'] - item['value'])
            hourly_errors[f"hour_{item['hour']}"][f"horizon_{item['horizon']}"].append(error)
        
        mae_results = {
            hour: {h: self._calculate_mae(e) for h, e in horizons.items()}
            for hour, horizons in hourly_errors.items()
        }
        return mae_results

    def save_to_sheet(self, writer, model_results_dict):
        first_result = next(iter(model_results_dict.values()), {})
        is_nested_dict = isinstance(first_result, dict) and \
                         isinstance(next(iter(first_result.values()), None), dict)

        if self.type == 'hourly' and is_nested_dict:
            self._create_hourly_comparison_sheet(writer, self.name, model_results_dict)
        else:
            super().save_to_sheet(writer, model_results_dict)

    def _create_hourly_comparison_sheet(self, writer, sheet_name, results_dict):
        sheet_name = sheet_name[:31]
        try:
            all_dfs = []
            model_names = list(results_dict.keys())
            if not model_names: return

            ref_index = pd.DataFrame.from_dict(results_dict[model_names[0]], orient='index').index

            for i, name in enumerate(model_names):
                df = pd.DataFrame.from_dict(results_dict[name], orient='index')
                df.columns = pd.MultiIndex.from_product([[name], df.columns])
                all_dfs.append(df)
                
                if i < len(model_names) - 1:
                    sep_col = pd.MultiIndex.from_tuples([(f'sep_{i}', '')])
                    all_dfs.append(pd.DataFrame('', index=ref_index, columns=sep_col))
                    
            combined_df = pd.concat(all_dfs, axis=1)
            styler = combined_df.style.background_gradient(cmap='RdYlGn_r', axis=0) # Color per column
            styler.to_excel(writer, sheet_name=sheet_name, float_format="%.2f")
            print(f"[dim]Sheet '{sheet_name}' (Hourly Format) created by {self.__class__.__name__}.")
        except Exception as e:
            print(f"[red]Could not create hourly sheet '{sheet_name}'. Error: {e}")