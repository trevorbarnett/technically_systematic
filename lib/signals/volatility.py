# volatility.py
from lib.signals.base_signal import SignalGenerator
import pandas as pd

class VolatilitySignal(SignalGenerator):
 def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Compute volatility signal for a single asset group.
    
    Args:
        data (pd.DataFrame): Data for a single asset group.
        name (str): Output column name for the signal.
        **kwargs: Additional parameters for signal computation and dependencies.
            - window (int): Lookback window for rolling standard deviation.
            - dependency_signals (dict): Upstream dependency results (optional).
    
    Returns:
        pd.DataFrame: DataFrame containing the volatility signal.
    """
    # Use upstream dependencies if provided
    dependencies = kwargs.get("dependency_signals", {})
    for dep_name, dep_data in dependencies.items():
        # Merge each dependency into the data
        data = data.merge(dep_data, on=["datetime", "asset"], how="left")

    # Compute the rolling standard deviation (volatility)
    window = kwargs.get("window", 30)
    data[name] = data['price'].pct_change().rolling(window=int(window)).std()

    return data[['asset', 'datetime', name]]