# mock_price_series_loader.py
import pandas as pd
from lib.data.base_loader import BaseDataLoader

class MockPriceSeriesLoader(BaseDataLoader):
  def __init__(self, config=None):
    self.config = config

  def load_data(self) -> pd.DataFrame:
    """
    Simulate loading price series data.
    """
    return pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "asset": ["AAPL", "AAPL", "GOOG"],
        "close": [150, 152, 2800]
    })

  def associate(self, primary: pd.DataFrame, secondary: pd.DataFrame) -> pd.DataFrame:
    """
    For price_series, association is unnecessary.
    """
    return primary
