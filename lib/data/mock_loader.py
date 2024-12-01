import pandas as pd
from lib.data.base_loader import BaseDataLoader

class MockLoader(BaseDataLoader):
  def __init__(self, config=None):
    self.config = config

  def load_data(self) -> pd.DataFrame:
    """
    Simulate loading data for testing purposes.
    """
    return pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-02"],
        "asset": ["AAPL", "GOOG"],
        "value": [100, 200]
    })