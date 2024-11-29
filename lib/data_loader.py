import importlib
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
