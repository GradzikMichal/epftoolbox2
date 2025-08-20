import pandas as pd
from rich import print

class EvaluatorPipeline:
    def __init__(self, horizon, data, testPeriodStart, testPeriodEnd):
        self.horizon = horizon
        self.data = data
        self.testPeriodStart = testPeriodStart
        self.testPeriodEnd = testPeriodEnd
        self.models = []
        self.evaluators = []

    def add_model(self, model):
        self.models.append(model)

    def add_evaluator(self, evaluator):
        self.evaluators.append(evaluator)

    def execute(self, output_filepath="results.xlsx"):

        print("[dim]Running all models.")
        model_outputs = {
            model.name: model.run(self.horizon, self.data, self.testPeriodStart, self.testPeriodEnd)
            for model in self.models
        }
        print("[dim]Model runs complete.")

        try:
            with pd.ExcelWriter(output_filepath, engine='openpyxl') as writer:
                self._create_info_sheet(writer)

                for evaluator in self.evaluators:
                    evaluator_name = evaluator.name
                    print(f"[dim]Processing results with evaluator: '{evaluator_name}'...")

                    model_results = {
                        model_name: evaluator.evaluate(output)
                        for model_name, output in model_outputs.items()
                    }
                    
                    evaluator.save_to_sheet(writer, model_results)

            print(f"[green]Successfully saved all results to {output_filepath}")

        except Exception as e:
            print(f"[red]An error occurred while saving the Excel file: {e}")

    def _create_info_sheet(self, writer):
        info_data = [
            {'Parameter': 'Test Period Start', 'Value': self.testPeriodStart},
            {'Parameter': 'Test Period End', 'Value': self.testPeriodEnd},
            {'Parameter': 'Forecast Horizon', 'Value': f'{self.horizon} steps'},
        ]
        
        for model in self.models:
            model_name = model.name
            info_data.append({'Parameter': '---', 'Value': f'--- {model_name} ---'})
            info_data.append({'Parameter': 'Predictors', 'Value': str(model.predictors)})
            model_params = model.modelParams
            for key, val in model_params.items():
                info_data.append({'Parameter': key, 'Value': str(val)})

        df_info = pd.DataFrame(info_data)
        df_info.to_excel(writer, sheet_name="Models Info", index=False)
        print("[dim]Sheet 'Models Info' created.")