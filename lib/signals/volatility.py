# volatility.py
from lib.signals.base_signal import SignalGenerator
import pandas as pd

class VolatilitySignal(SignalGenerator):
    def _compute(self, group: pd.DataFrame, name: str, window: int = 30, **kwargs) -> pd.DataFrame:
        """Generate volatility signal (rolling standard deviation)."""
        volatility = group['price'].pct_change().rolling(window=int(window)).std()
        group[name] = volatility
        return group[['asset', name]]
