from abc import ABC, abstractmethod
import pandas as pd

class SignalGenerator(ABC):

  def generate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    if 'asset' not in data.columns:
      raise ValueError("Input data must include 'asset' column")
    result = data.groupby('asset').apply(
      lambda group: self._compute(group, name=name, **kwargs)
    )
    return result.reset_index(drop=True)

  @abstractmethod
  def _compute(self, data:pd.DataFrame, name: str,  **kwargs) -> pd.DataFrame:
    """Compute the signal based on the input data.

    Args:
        data (pd.DataFrame): Input market data 
        name (str): Custom name for the signal column
        **kwargs: Additional parameters for the signal computation

    Returns:
        pd.DataFrame: Dataframe with the generated signals
    """
    pass