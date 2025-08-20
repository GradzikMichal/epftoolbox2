import pandas as pd
from abc import ABC, abstractmethod
from rich import print

class BaseEvaluator(ABC):
    def __init__(self, name='Evaluator'):
        self.name = name[:31] #excelmlimit
        
        
    @abstractmethod
    def evaluate(self, data):
        pass

    def save_to_sheet(self, writer, model_results_dict):
        try:
            df = pd.DataFrame(model_results_dict)
            styled_df = df.style.background_gradient(cmap='RdYlGn_r', axis=1)
            styled_df.to_excel(writer, sheet_name=self.name, float_format="%.4f")
            print(f"[dim]Sheet '{self.name}' created by {self.__class__.__name__}.")
        except Exception as e:
            print(f"[red]Could not create sheet '{self.name}'. Error: {e}")