import importlib
import pandas as pd
from typing import Dict

from lib.config import DataLoaderConfig
from lib.data.base_loader import BaseDataLoader

def create_loader(loader_config: DataLoaderConfig) -> BaseDataLoader:
  """
  Create a data loader based on the configuration.

  Args:
      loader_config (dict): Loader-specific configuration.

  Returns:
      BaseDataLoader: An instance of the appropriate loader class.
  """
  module_name = loader_config.module
  class_name = loader_config.class_name
  loader_module = importlib.import_module(module_name)
  loader_class = getattr(loader_module, class_name)
  return loader_class(loader_config)

def load_data(data_loaders: Dict[str,BaseDataLoader]) -> pd.DataFrame:
    """
    Load and integrate datasets with associations.

    Returns:
        pd.DataFrame: Integrated DataFrame with all datasets merged.
    """
    # Load primary dataset
    primary_data = data_loaders["price_series"].load_data()

    # Process and associate secondary datasets
    for name, loader in data_loaders.items():
      if name == "price_series":
          continue  # Skip the primary dataset

      secondary_data = loader.load_data()
      primary_data = loader.associate(primary_data, secondary_data)

    return primary_data