# local_cache.py
import os
import pandas as pd
from lib.cache.base_cache import BaseCache
from lib.config import CacheConfig

class LocalCache(BaseCache):
  def __init__(self, config:CacheConfig):
    super().__init__(config)
    self.cache_dir = config.params.get("cache_dir","cache")
    os.makedirs(self.cache_dir, exist_ok=True)

  def get_cache_path(self, key: str) -> str:
    return os.path.join(self.cache_dir, f"{key}.pkl")

  def save(self, key: str, data: pd.DataFrame):
    filepath = self.get_cache_path(key)
    data.to_pickle(filepath)
    return filepath

  def load(self, key: str) -> pd.DataFrame:
    filepath = self.get_cache_path(key)
    return pd.read_pickle(filepath)

  def exists(self, key: str) -> bool:
    return os.path.exists(self.get_cache_path(key))
