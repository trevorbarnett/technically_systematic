from dask import delayed
from dask.delayed import Delayed
from typing import Dict
import pandas as pd

from lib.calculations.base_calculation import DataCalculation
from lib.config import DataCalculationConfig

def execute_partitions(calculator: DataCalculation, partitions: Dict[str, pd.DataFrame], calculator_config: DataCalculationConfig, results: Dict[str,pd.DataFrame]) -> Dict[str, Delayed]:
  partition_results = {}
  for partition_name, partition_data in partitions.items():
    # Gather upstream dependency results for this partition
    dependencies = resolve_dependencies(partition_name, calculator_config, results)
    partition_data = merge_dependencies(partition_data, dependencies)

    # Create Dask task
    task = delayed(calculator.run)(
        partition_data, name=calculator_config.output_name, **calculator_config.params
    )
    partition_results[partition_name] = task  
  return {k: v.compute() for k, v in partition_results.items()}


def resolve_dependencies(partition_name: str, calculation_config: DataCalculationConfig, results: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
  """Gather the upstream dependency results for a given partition

  Args:
      partition_name (str): Name of the partition
      calculation_config (DataCalculationConfig): Calculator config
      results (Dict[str, pd.DataFrame]): Results

  Returns:
      Dict[str, Delayed]: Dependency tasks
  """
  dependencies = {}
  for dep_name in calculation_config.dependencies:
    dep_key = (dep_name, partition_name)
    dep_result = results.get(dep_key)
    if isinstance(dep_result, Delayed):  # Check for dask.delayed.Delayed objects
      dependencies[dep_name] = dep_result.compute()
    else:
      dependencies[dep_name] = dep_result
  return dependencies

def merge_dependencies(partition_data: pd.DataFrame, dependencies: Dict[str, pd.DataFrame]) -> pd.DataFrame:
  """Merge dependencies into partition data

  Args:
      partition_data (pd.DataFrame): Partition data
      dependencies (Dict[str, pd.DataFrame]): Dependencies

  Returns:
      pd.DataFrame: Merged data
  """
  # Merge dependencies into the partition data
  for _, dep_data in dependencies.items():
    if dep_data is not None:
      if isinstance(dep_data,tuple):
        dep_data,_ = dep_data
      partition_data = partition_data.merge(dep_data, on=["datetime", "asset"], how="left")
  return partition_data