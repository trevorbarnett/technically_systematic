from lib.signals.base_signal import SignalGenerator
import pandas as pd

class ReversionSignal(SignalGenerator):
  def _compute(self, group: pd.DataFrame, name: str, short_window: int = 5, long_window: int = 60, **kwargs) -> pd.DataFrame:
    short_ma = group['price'].rolling(window=int(short_window)).mean()
    long_ma = group['price'].rolling(window=int(long_window)).mean()
    reversion = short_ma - long_ma
    group[name] = reversion
    return group[['asset', name]]  