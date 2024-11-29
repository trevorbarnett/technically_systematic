from lib.calculations.base_calculation import DataCalculation
import pandas as pd

class ReversionSignal(DataCalculation):
  def calculate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Calculate a reversion signal based on short-term and long-term moving averages.

    Args:
        data (pd.DataFrame): Input data.
        name (str): Name of the output column.
        **kwargs: Additional parameters, including:
            - column: The input column (default: "close").
            - short_window: Lookback window for short-term average.
            - long_window: Lookback window for long-term average.

    Returns:
        pd.DataFrame: DataFrame with the reversion signal.
    """
    column = kwargs.get("column", "close")
    short_window = int(kwargs.get("short_window", 5))
    long_window = int(kwargs.get("long_window", 20))

    data = self.extract_columns(data,["datetime","asset",column])
    # Compute short- and long-term moving averages
    short_ma = data[column].rolling(window=short_window).mean()
    long_ma = data[column].rolling(window=long_window).mean()

    # Calculate the reversion signal
    data.loc[:, name] = short_ma - long_ma

    return data.loc[:, ["datetime", "asset", name]]
