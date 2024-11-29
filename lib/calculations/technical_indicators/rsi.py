from lib.calculations.base_calculation import DataCalculation
import pandas as pd

class RSI(DataCalculation):
  def calculate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Calculate the Relative Strength Index (RSI).

    Args:
        data (pd.DataFrame): Input data.
        name (str): Name of the output column.
        **kwargs: Additional parameters, including:
            - column: The input column (default: "close").
            - window: The lookback period for RSI.

    Returns:
        pd.DataFrame: DataFrame with RSI values.
    """
    column = kwargs.get("column", "close")
    window = int(kwargs.get("window", 14))

    data = self.extract_columns(data,["datetime", "asset", column])

    delta = data[column].diff()
    gain = delta.where(delta > 0, 0).rolling(window=window).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=window).mean()
    rs = gain / loss
    data.loc[:, name] = 100 - (100 / (1 + rs))

    return data.loc[:, ["datetime", "asset", name]]