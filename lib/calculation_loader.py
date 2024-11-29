import importlib
from typing import List
from lib.config import DataCalculationConfig

def load_and_register_calculations(manifest: List[DataCalculationConfig]):
  signal_classes = {}
  for signal_def in manifest:
    module = importlib.import_module(signal_def.module)
    signal_class = getattr(module, signal_def.class_name)
    signal_classes[signal_def.class_name] = signal_class
  return signal_classes