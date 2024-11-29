from lib.calculations.base_calculation import DataCalculation
import pandas as pd

class MovingAverage(DataCalculation):
  def calculate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Calculate a moving average for a given column.

    Args:
        data (pd.DataFrame): Input data.
        name (str): Name of the output column.
        **kwargs: Additional parameters, including:
            - column: The input column (default: "close").
            - window: The rolling window size.

    Returns:
        pd.DataFrame: DataFrame with the moving average.
    """    
    column = kwargs.get("column", "close")
    window = int(kwargs.get("window", 60))
    
    data = self.extract_columns(data,["datetime", "asset", column])

    data.loc[:,name] = data[column].rolling(window=window).mean()
    return data.loc[:,['asset', 'datetime', name]]
