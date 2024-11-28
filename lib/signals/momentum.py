from lib.signals.base_signal import SignalGenerator
import pandas as pd

class MomentumSignal(SignalGenerator):
  def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """Compute momentum signal for a single asset group."""
    momentum = data['price'].pct_change().rolling(window=int(kwargs['window'])).mean()
    data[name] = momentum
    return data[['asset', 'datetime', name]]
