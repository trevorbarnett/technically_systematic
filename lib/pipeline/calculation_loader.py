import importlib
from typing import List

from lib.config import DataCalculationDefinition

def load_and_register_calculations(manifest: List[DataCalculationDefinition]):
  calculation_classes = {}
  for calc_def in manifest:
    module = importlib.import_module(calc_def.module)
    calculation_class = getattr(module, calc_def.class_name)
    calculation_classes[calc_def.class_name] = calculation_class
  return calculation_classes