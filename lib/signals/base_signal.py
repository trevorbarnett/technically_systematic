from abc import ABC, abstractmethod
from typing import Dict, List
import pandas as pd
import hashlib

class SignalGenerator(ABC):
  def __init__(self, cache=None):
    """
    Initialize the signal generator.
    
    Args:
        cache: An instance of a Cache implementation.
    """
    self.cache = cache
  @abstractmethod
  def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """Generate the signal based on the input data."""
    pass
  def extract_columns(self, data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Extract the required columns

    Args:
        data (pd.DataFrame): The input data.
        columns (List[str]): List of required column names

    Returns:
        pd.DataFrame: A DataFrame with only the requested columns
    """
    missing_cols = [col for col in columns if col not in data.columns]
    if missing_cols:
      raise ValueError(f"Missing required columns: {missing_cols}")
    return data.loc[:,columns]

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

  def run(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """Generate the signal, using cache if available."""
    # Create a unique cache key based on signal name and data hash
    data_hash = hashlib.md5(pd.util.hash_pandas_object(data, index=True).values).hexdigest()
    cache_key = f"{name}_{data_hash}"
    if self.cache and self.cache.exists(cache_key):
      return self.cache.load(cache_key)

    # Generate the signal and cache the result
    result = self.generate(data, name, **kwargs)
    if self.cache:
      self.cache.save(cache_key, result)
    return result