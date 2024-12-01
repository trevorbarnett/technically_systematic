# mock_output.py
from lib.output.base_output import Output
import pandas as pd

class MockOutput(Output):
  def __init__(self, config=None):
    self.config = config
    self.data = []  # Store written data for validation

  def write(self, data: pd.DataFrame):
    """
    Simulate writing data to an output.

    Args:
        data (pd.DataFrame): Data to write.
    """
    self.data.append(data)
