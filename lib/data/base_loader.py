# data_loader.py
from abc import ABC, abstractmethod
import pandas as pd
from ..config import DataLoaderConfig, DataFillType, DataJoinType

class BaseDataLoader(ABC):
  def __init__(self, config: DataLoaderConfig):
    """
    Initialize the data loader.

    Args:
        config (dict): Loader-specific configuration.
    """
    self.config = config

  @abstractmethod
  def load_data(self) -> pd.DataFrame:
    """Load data into a Pandas DataFrame."""
    pass

  def associate(self, primary: pd.DataFrame, secondary: pd.DataFrame) -> pd.DataFrame:
    """
    Associate secondary data with the primary data using a point-in-time mapping.

    Args:
        primary (pd.DataFrame): Primary dataset (e.g., price series).
        secondary (pd.DataFrame): Secondary dataset (e.g., sector data).

    Returns:
        pd.DataFrame: Merged DataFrame with secondary data associated.
    """
    # Get association config
    association_config = self.config.association
    if association_config:
      join_keys = association_config.on
      how = association_config.how
      fill_method = association_config.fill_method
    else:
      join_keys = ["asset", "datetime"]  # Keys for joining
      how = DataJoinType.Left
      fill_method = DataFillType.ForwardFill  # Fill method for alignment

    # Perform the join
    associated_data = pd.merge(primary, secondary, on=join_keys, how=how.value, suffixes=("","_secondary"))

    # Align data if necessary
    if fill_method:
      if fill_method == DataFillType.BackwardFill:
        associated_data = associated_data.bfill()
      elif fill_method == DataFillType.ForwardFill:
        associated_data = associated_data.ffill()
      else:
        raise ValueError(f"Invalid fill type: {fill_method}")


    return associated_data
  def validate_data(self, data: pd.DataFrame):
    """
    Validate the loaded data for required columns and datetime granularity.

    Args:
        data (pd.DataFrame): The loaded data.

    Raises:
        ValueError: If required columns are missing or datetime granularity is incorrect.
    """
    required_columns = self.config.required_columns
    datetime_column = self.config.datetime_column
    granularity = self.config.datetime_granularity

    # Check for required columns
    missing_cols = [col for col in required_columns if col not in data.columns]
    if missing_cols:
      raise ValueError(f"Missing required columns: {missing_cols}")

    # Validate datetime granularity
    if granularity and datetime_column in data.columns:
      unique_datetime = data[datetime_column].unique()
      unique_datetime_sorted = unique_datetime[unique_datetime.argsort()]
      dt_diff = (unique_datetime_sorted - unique_datetime_sorted.shift()).dropna()
      actual_granularity = dt_diff.value_counts().idxmax()
      if actual_granularity != pd.to_timedelta(str(granularity)):
        raise ValueError(f"Expected datetime granularity '{granularity}', got '{actual_granularity}'")

    return data