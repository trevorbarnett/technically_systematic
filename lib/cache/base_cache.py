# cache.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseCache(ABC):
  """Abstract base class for caching signal results."""
  
  @abstractmethod
  def save(self, key: str, data: pd.DataFrame):
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
