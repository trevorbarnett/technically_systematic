from lib.signals.base_signal import SignalGenerator
import pandas as pd

class MomentumSignal(SignalGenerator):
  def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """Compute momentum signal for a single asset group."""
    column = kwargs.get("column", "close")
    window = int(kwargs.get("window", 60))
    
    data = self.extract_columns(data,["datetime", "asset", column])

    data.loc[:,name] = data[column].pct_change().rolling(window=window).mean()
    return data.loc[:,['asset', 'datetime', name]]
