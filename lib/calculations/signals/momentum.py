# signals/momentum_signal.py
from lib.calculations.base_calculation import DataCalculation
import pandas as pd

class MomentumSignal(DataCalculation):
  def calculate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Calculate a momentum signal based on price changes.

    Args:
        data (pd.DataFrame): Input data.
        name (str): Name of the output column.
        **kwargs: Additional parameters, including:
            - column: The input column (default: "close").
            - window: The rolling window size.

    Returns:
        pd.DataFrame: DataFrame with the momentum signal.
    """
    column = kwargs.get("column", "close")
    window = kwargs.get("window", 60)

    data = self.extract_columns(data,["datetime", "asset", column])

    data.loc[:, name] = data[column].pct_change().rolling(window=window).mean()
    return data.loc[:, ["datetime", "asset", name]]
