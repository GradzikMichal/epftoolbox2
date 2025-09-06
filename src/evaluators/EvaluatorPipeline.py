import os
import pandas as pd
from rich import print
import glob

class EvaluatorPipeline:
    def __init__(self, data, testPeriodStart, testPeriodEnd, target="load", horizon=1, details=False):
        self.horizon = horizon
        self.data = data
        self.testPeriodStart = testPeriodStart
        self.target = target
        self.testPeriodEnd = testPeriodEnd
        self.details = details
        self.models = []
        self.evaluators = []

    def add_model(self, model):
        self.models.append(model)
        
    def clear_cache(self):
        for file in glob.glob('./results/*'):
            os.remove(file)

    def add_evaluator(self, evaluator):
        self.evaluators.append(evaluator)

    def execute(self, output_filepath="results.xlsx"):
        print("[dim]Running all models.")
        model_outputs = {
            model.name: model.run(self.horizon, self.data, self.testPeriodStart, self.testPeriodEnd, self.target)
            for model in self.models
        }
        print("[dim]Model runs complete.")

        with pd.ExcelWriter(output_filepath, engine='xlsxwriter') as writer:
            self._create_info_sheet(writer)

            if self.details:
                print("[dim]Generating combined details sheet...")
                details_df = self._create_base_details_df(model_outputs)
                
                if not details_df.empty:
                    for evaluator in self.evaluators:
                        if evaluator.type == 'details':
                            details_df = evaluator.append_metrics_to_df(details_df)
                    
                    details_df.to_excel(writer, sheet_name="Detailed_Comparison", index=False, float_format="%.2f")
                    print("[green]Successfully created combined comparison sheet: 'Detailed_Comparison'")
                else:
                    print("[yellow]Skipping details sheet generation as no model data was found.")

            for evaluator in self.evaluators:
                if evaluator.type != 'details':
                    evaluator_name = evaluator.name
                    print(f"[dim]Processing results with standalone evaluator: '{evaluator_name}'...")
                    model_results = {
                        model_name: evaluator.evaluate(output)
                        for model_name, output in model_outputs.items()
                    }
                    evaluator.save_to_sheet(writer, model_results)

        print(f"[green]Successfully saved all results to {output_filepath}")

    def _create_base_details_df(self, model_outputs):
        all_model_dfs = []
        for model_name, model_data in model_outputs.items():
            if not model_data: continue
            df = pd.DataFrame(model_data)
            df['datetime'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'], unit='h')
            df.rename(columns={'prediction': f'prediction_{model_name}'}, inplace=True)
            df.set_index(['datetime', 'hour', 'horizon', 'value'], inplace=True)
            all_model_dfs.append(df[[f'prediction_{model_name}']])
        
        if not all_model_dfs: return pd.DataFrame()

        combined_df = pd.concat(all_model_dfs, axis=1).reset_index()
        combined_df['date'] = combined_df['datetime'].dt.date
        
        model_pred_cols = sorted([col for col in combined_df.columns if col.startswith('prediction_')])
        final_columns = ['datetime', 'date', 'hour', 'horizon', 'value'] + model_pred_cols
        return combined_df[final_columns]

    def _create_info_sheet(self, writer):
        info_data = [{'Parameter': 'Test Period Start', 'Value': self.testPeriodStart}, {'Parameter': 'Test Period End', 'Value': self.testPeriodEnd}, {'Parameter': 'Forecast Horizon', 'Value': f'{self.horizon} steps'},]
        for model in self.models:
            model_name = model.name
            info_data.append({'Parameter': '---', 'Value': f'--- {model_name} ---'})
            info_data.append({'Parameter': 'Predictors', 'Value': str(model.processColumns(model.predictors,{"hour": 0,"dayInTestingPeriod": 0,"datasetOffset": 0,"horizon": 0,"target": self.target,"trainingWindow": 0,}))})
            model_params = model.modelParams
            for key, val in model_params.items():
                info_data.append({'Parameter': key, 'Value': str(val)})
        df_info = pd.DataFrame(info_data)
        df_info.to_excel(writer, sheet_name="Models Info", index=False)
        print("[dim]Sheet 'Models Info' created.")