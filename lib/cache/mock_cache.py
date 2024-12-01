# mock_cache.py
from lib.cache.base_cache import BaseCache
import pandas as pd

class MockCache(BaseCache):
  def __init__(self, config=None):
    self.config = config
    self.storage = {}  # In-memory storage

  def exists(self, key: str) -> bool:
    """
    Check if a cache key exists.

    Args:
        key (str): Cache key.

    Returns:
        bool: True if the key exists, False otherwise.
    """
    return key in self.storage

  def load(self, key: str) -> pd.DataFrame:
    """
    Load data from the cache.

    Args:
        key (str): Cache key.

    Returns:
        pd.DataFrame: Cached data.
    """
    return self.storage.get(key)

  def save(self, key: str, data: pd.DataFrame):
    """
    Save data to the cache.

    Args:
        key (str): Cache key.
        data (pd.DataFrame): Data to cache.
    """
    self.storage[key] = data
  def get_cache_path(self, key: str) -> str:
    return key
