# local_cache.py
import os
import pandas as pd
from lib.cache.base_cache import BaseCache

class LocalCache(BaseCache):
  def __init__(self, cache_dir="cache"):
    self.cache_dir = cache_dir
    os.makedirs(self.cache_dir, exist_ok=True)

  def save(self, key: str, data: pd.DataFrame):
    filepath = os.path.join(self.cache_dir, f"{key}.pkl")
    data.to_pickle(filepath)

  def load(self, key: str) -> pd.DataFrame:
    filepath = os.path.join(self.cache_dir, f"{key}.pkl")
    return pd.read_pickle(filepath)

  def exists(self, key: str) -> bool:
    filepath = os.path.join(self.cache_dir, f"{key}.pkl")
    return os.path.exists(filepath)
