# volatility.py
from lib.signals.base_signal import SignalGenerator
import pandas as pd

class VolatilitySignal(SignalGenerator):
 def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Compute volatility signal for a single asset group.
    
    Args:
        data (pd.DataFrame): Input data.
        name (str): Name of the output column.
        **kwargs: Additional parameters, including:
            - high_column: The high price column (default: 'high').
            - low_column: The low price column (default: 'low').
            - window: The rolling window size.

    Returns:
        pd.DataFrame: DataFrame containing the volatility signal.
    """
    high_column = kwargs.get("high_column", "high")
    low_column = kwargs.get("low_column", "low")
    window = int(kwargs.get("window", 30))

    data = self.extract_columns(data, ["datetime", "asset", high_column, low_column])
    data.loc[:,name] = (data[high_column] - data[low_column]).rolling(window=window).mean()
    return data.loc[:,['datetime', 'asset', name]]