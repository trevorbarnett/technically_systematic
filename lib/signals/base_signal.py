from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd

class SignalGenerator(ABC):

  @abstractmethod
  def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
      """Generate the signal based on the input data."""
      pass
  def partition(self, data: pd.DataFrame, **kwargs) -> Dict[str, pd.DataFrame]:
    """Partition the data for processing

    Args:
        data (pd.DataFrame): Input data
        **kwargs: Additional parameters for partitioning

    Returns:
        Dict[str, pd.DataFrame]: Partitioned data where keys are partition names (e.g. asset names or other groupings).
    """

    # Default partitioning: per asset
    return {asset: group for asset, group in data.groupby("asset")}