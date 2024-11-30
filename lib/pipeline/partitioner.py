from typing import Dict
import pandas as pd


from lib.calculations.base_calculation import DataCalculation
from lib.config import DataCalculationConfig


def partition_data(calculator: DataCalculation, data: pd.DataFrame, calculation_config: DataCalculationConfig) -> Dict[str,pd.DataFrame]:
  """Partition the data based on the caculation's requirements

  Args:
      calculator (DataCalculation): The calculator to use
      data (pd.DataFrame): The data to partition
      calculation_config (DataCalculationConfig): The calculator's config

  Raises:
      ValueError: If the calculator can't partition the data

  Returns:
      Dict[str,pd.DataFrame]: Partitioned data
  """
  return calculator.partition(data,**calculation_config.params)
