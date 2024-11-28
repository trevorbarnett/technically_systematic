import importlib
from typing import List
from lib.signal_registry import SignalRegistry
from lib.config import SignalDefinition

def load_and_register_signals(manifest: List[SignalDefinition]):
  signal_classes = {}
  for signal_def in manifest:
    module = importlib.import_module(signal_def.module)
    signal_class = getattr(module, signal_def.class_name)
    signal_classes[signal_def.class_name] = signal_class
  return signal_classes