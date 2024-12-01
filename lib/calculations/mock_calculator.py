# mock_calculation.py
import pandas as pd
from lib.calculations.base_calculation import DataCalculation

class MockCalculation(DataCalculation):
  def calculate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Simulate performing a calculation.
    """
    # Add a simple calculation for demonstration (e.g., calculate mean of 'close')
    data[f"{name}_calc"] = data["close"].rolling(window=2).mean()
    return data
