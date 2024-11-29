# base_output.py
from lib.config import OutputConfig
from abc import ABC, abstractmethod
import pandas as pd

class Output(ABC):
  def __init__(self, config: OutputConfig):
    """
    Base class for pipeline outputs.

    Args:
        config (dict): Output-specific configuration.
    """
    self.config = config

  @abstractmethod
  def write(self, data: pd.DataFrame, **kwargs):
    """
    Write data to the output destination.

    Args:
        data (pd.DataFrame): DataFrame to be written.
    """
    pass
