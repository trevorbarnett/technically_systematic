from lib.signals.base_signal import SignalGenerator
import pandas as pd

class MomentumSignal(SignalGenerator):
  def _compute(self, group:pd.DataFrame, name: str, window: int=60, **kwargs) -> pd.DataFrame:
    momentum = group['price'].pct_change().rolling(window=int(window)).mean()
    group[name] =  momentum
    return group[['asset', name]]
