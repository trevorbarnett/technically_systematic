from lib.signals.base_signal import SignalGenerator
import pandas as pd
from typing import Dict

class ReversionSignal(SignalGenerator):
  def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Compute mean reversion signal for a single asset group.
    
    Args:
        data (pd.DataFrame): Data for a single asset group.
        name (str): Output column name for the signal.
        **kwargs: Additional parameters for signal computation and dependencies.
            - short_window (int): Lookback window for short-term average.
            - long_window (int): Lookback window for long-term average.
            - momentum_signal (pd.DataFrame): Upstream dependency result (optional).
    
    Returns:
        pd.DataFrame: DataFrame containing the reversion signal.
    """
    # Use upstream dependency if provided
    momentum = kwargs.get("momentum_signal")
    if momentum is not None:
      # Merge momentum signal into the data if it exists
      data = data.merge(momentum, on=["datetime", "asset"], how="left")

    # Compute the reversion signal
    short_window = kwargs.get("short_window", 5)
    long_window = kwargs.get("long_window", 60)

    short_ma = data['price'].rolling(window=int(short_window)).mean()
    long_ma = data['price'].rolling(window=int(long_window)).mean()
    data[name] = short_ma - long_ma

    return data[['asset', 'datetime', name]]
