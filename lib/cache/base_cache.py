# cache.py
from abc import ABC, abstractmethod
from lib.config import CacheConfig
import pandas as pd
import hashlib

class BaseCache(ABC):
  """Abstract base class for caching signal results."""
  
  def __init__(self, config: CacheConfig):
    self.config = config

  def generate_key(self, identifier: str) -> str:
    """
    Generate a unique cache key using hashing.

    Args:
        identifier (str): A unique identifier for the cache entry.

    Returns:
        str: A unique hash key for the cache entry.
    """
    return hashlib.sha256(identifier.encode()).hexdigest()
  
  @abstractmethod
  def get_cache_path(self, key:str) -> str:
    """
    Get the file path or URI for the given cache key.

    Args:
        key (str): Unique cache key.

    Returns:
        str: File path or URI.
    """
    pass
  @abstractmethod
  def save(self, key: str, data: pd.DataFrame) -> str:
    """Save data to the cache."""
    pass

  @abstractmethod
  def load(self, key: str) -> pd.DataFrame:
    """Load data from the cache."""
    pass

  @abstractmethod
  def exists(self, key: str) -> bool:
    """Check if a cached result exists."""
    pass
