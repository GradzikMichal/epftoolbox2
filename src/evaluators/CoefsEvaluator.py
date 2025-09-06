import pandas as pd
import collections
from .BaseEvaluator import BaseEvaluator
from rich import print

class CoefsEvaluator(BaseEvaluator):
    def __init__(self, type='daily', name='Coefficients Report'):
        super().__init__(name)
        self.type = type

    def append_metrics_to_df(self, base_df, model_outputs):
        print(f"[dim]Appending metrics from '{self.name}'...")
        
        all_coef_rows = []
        for model_name, model_data in model_outputs.items():
            if not model_data or 'coefs' not in model_data[0]:
                print(f"[yellow]'{model_name}' has no coefficient data. Skipping in '{self.name}'.[/yellow]")
                continue

            for item in model_data:
                predictors, coef_values = self._get_validated_coefs(item)
                if predictors is None:
                    continue

                row = {
                    'datetime': pd.to_datetime(item['date']) + pd.to_timedelta(item['hour'], unit='h')
                }
                for predictor, coef_value in zip(predictors, coef_values):
                    row[f"coef_{model_name}_{predictor}"] = coef_value
                all_coef_rows.append(row)

        if not all_coef_rows:
            print(f"[yellow]No valid coefficient data found across all models for '{self.name}'.[/yellow]")
            return base_df

        print("[dim]   -> Creating a single comprehensive coefficient DataFrame...")
        all_coefs_df = pd.DataFrame(all_coef_rows)
        all_coefs_df = all_coefs_df.groupby('datetime').first().reset_index()

        print("[dim]   -> Merging coefficients into the main details table...")
        base_df['datetime'] = pd.to_datetime(base_df['datetime'])
        final_df = pd.merge(base_df, all_coefs_df, on='datetime', how='left')
        
        return final_df

    def evaluate(self, data):
        if not data: return pd.DataFrame()

        long_format_df = self._flatten_to_long_format(data)
        if long_format_df.empty: return pd.DataFrame()

        if self.type == 'all':
            return long_format_df.pivot_table(index='predictor_name', values='coef_value', aggfunc='mean').rename(columns={'coef_value': 'mean_coef'})
        elif self.type == 'daily':
            return long_format_df.pivot_table(index='horizon', columns='predictor_name', values='coef_value', aggfunc='mean')
        elif self.type == 'hourly':
            return long_format_df.pivot_table(index=['horizon', 'hour'], columns='predictor_name', values='coef_value', aggfunc='mean')
        return pd.DataFrame()

    def save_to_sheet(self, writer, model_results_dict):
        all_dfs = []
        model_names = [name for name, df in model_results_dict.items() if not df.empty]
        
        for i, model_name in enumerate(model_names):
            result_df = model_results_dict[model_name]
            
            result_df.columns = pd.MultiIndex.from_product([[model_name], result_df.columns])
            all_dfs.append(result_df)
            
            if i < len(model_names) - 1:
                separator = pd.DataFrame('', index=result_df.index, 
                                         columns=pd.MultiIndex.from_tuples([('|', f'sep_{i}')]))
                all_dfs.append(separator)
        
        if not all_dfs:
            print(f"[yellow]No coefficient data to save for sheet '{self.name}'.[/yellow]")
            return

        combined_df = pd.concat(all_dfs, axis=1)

        styler = combined_df.style

        for model_name in model_names:
            subset = pd.IndexSlice[:, model_name]
            styler = styler.background_gradient(cmap='viridis', subset=subset, axis=None)

        styler.to_excel(writer, sheet_name=self.name, float_format="%.4f")
        print(f"[dim]Sheet '{self.name}' created.")

    def _get_validated_coefs(self, item):
        predictors = item.get('predictors')
        coefs = item.get('coefs')

        if not predictors or coefs is None or len(coefs) == 0:
            return None, None

        if not isinstance(predictors, list):
            predictors = [predictors]
        
        if isinstance(coefs[0], list):
            coef_values = coefs[0]
        else:
            coef_values = coefs

        if len(predictors) != len(coef_values):
            return None, None
            
        return predictors, coef_values

    def _flatten_to_long_format(self, data):
        flat_data = []
        for item in data:
            predictors, coef_values = self._get_validated_coefs(item)
            if predictors is None:
                continue
            
            for predictor, coef_value in zip(predictors, coef_values):
                flat_data.append({
                    'hour': item.get('hour'),
                    'horizon': item.get('horizon'),
                    'predictor_name': predictor,
                    'coef_value': coef_value
                })
        
        return pd.DataFrame(flat_data)