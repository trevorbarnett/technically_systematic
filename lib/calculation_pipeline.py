from graphlib import TopologicalSorter
import importlib
from typing import Union
from dask import delayed, compute
from dask.delayed import Delayed
from dask.distributed import Client, LocalCluster
from lib.calculation_loader import load_and_register_calculations
from lib.config import PipelineConfig, DaskScheduler, CacheConfig
from lib.cache.base_cache import BaseCache
from lib.data_loader import create_loader
import pandas as pd

class CalculationPipeline:
  def __init__(self, config: PipelineConfig):
    """Initialize the calculation pipeline with a configuration

    Args:
        config (PipelineConfig): Configuration defining which calculations to load generate and their parameters
    """
    self.config = config
    self.cache = self._initialize_cache(config.cache)
    self.data_loaders = self._initialize_loaders(config.data_loaders)
    self.sorted_calculations = self._sort_calculations_by_dependencies()
    self.calculation_classes = load_and_register_calculations(config.calculations_manifest)
    self.dask_client = self._setup_dask() if config.dask.enabled else None

  def _initialize_cache(self,cache_config: Union[CacheConfig, None]) -> Union[BaseCache]:
    if cache_config == None:
      return None
    cache_module_name = cache_config.module
    cache_classname = cache_config.classname
    cache_module = importlib.import_module(cache_module_name)
    cache_class = getattr(cache_module, cache_classname)
    return cache_class(**cache_config.params)
  def _initialize_loaders(self, loaders_config: dict):
    """
    Initialize all data loaders.

    Args:
        loaders_config (dict): Configuration for all data loaders.

    Returns:
        dict: A dictionary of {dataset_name: DataLoader} instances.
    """
    return {
      name: create_loader(loader_config)
      for name, loader_config in loaders_config.items()
    }

  def _sort_calculations_by_dependencies(self):
    dag = TopologicalSorter()
    for calculation in self.config.calculations:
      dag.add(calculation.output_name, *calculation.dependencies)
  
    return list(dag.static_order()) 
  
  def _setup_dask(self):
    """Setup Dask client based on configuration
    """
    if self.config.dask.scheduler == DaskScheduler.Threads:
      # Local multithreaded scheduler
      return Client(processes=False, threads_per_worker=self.config.dask.num_workers or 4)
    elif self.config.dask.scheduler == DaskScheduler.Processes:
      # Local multiprocessing scheduler
      return Client(processes=True, n_workers = self.config.dask.num_workers or 4 )
    elif self.config.dask.scheduler == DaskScheduler.Distributed:
      # Distributed cluster setup
      cluster = LocalCluster(n_workers=self.config.dask.num_workers or 4) # TODO: Enable more than LocalCluster
      return Client(cluster)
    else:
      raise ValueError(f"Unssuprted Dask scheduler: {self.config.dask.scheduler}")
    

  def load_data(self) -> pd.DataFrame:
    """
    Load and integrate datasets with associations.

    Returns:
        pd.DataFrame: Integrated DataFrame with all datasets merged.
    """
    # Load primary dataset
    primary_data = self.data_loaders["price_series"].load_data()

    # Process and associate secondary datasets
    for name, loader in self.data_loaders.items():
      if name == "price_series":
          continue  # Skip the primary dataset

      secondary_data = loader.load_data()
      primary_data = loader.associate(primary_data, secondary_data)

    return primary_data
  def run(self) -> pd.DataFrame:
    results = {}

    data = self.load_data()

    for calculation_name in self.sorted_calculations:
      # Find the corresponding calculation config
      calculation_config = next(s for s in self.config.calculations if s.output_name == calculation_name)
      calculation_class = self.calculation_classes[calculation_config.name]
      calculator = calculation_class(cache=self.cache)
      
      # Partition data as required by the calculation
      partitions = calculator.partition(data, **calculation_config.params)

      partition_results = {}
      for partition_name, partition_data in partitions.items():
        # Gather upstream dependency results for this partition
        dependencies = {}
        for dep_name in calculation_config.dependencies:
          dep_key = (dep_name, partition_name)
          dep_result = results.get(dep_key)
      
          if isinstance(dep_result, Delayed):  # Check for dask.delayed.Delayed objects
            dependencies[dep_name] = dep_result.compute()
          else:
            dependencies[dep_name] = dep_result

        # Merge dependencies into the partition data
        for dep_name, dep_data in dependencies.items():
          if dep_data is not None:
            partition_data = partition_data.merge(dep_data, on=["datetime", "asset"], how="left")

        # Create Dask task
        task = delayed(calculator.run)(
            partition_data, name=calculation_config.output_name, **calculation_config.params
        )
        partition_results[partition_name] = task

      # Merge partitions for the current calculation
      if self.config.dask.enabled:
        computed_partitions = compute(*partition_results.values())
        merged_result = pd.concat(computed_partitions, ignore_index=True)
      else:
        computed_partitions = [task.compute() for task in partition_results.values()]
        merged_result = pd.concat(computed_partitions, ignore_index=True)

      # Save the merged result for this calculation
      results[calculation_name] = merged_result

      # Also, store individual partitions in case downstream calculations need them
      for partition_name, partition_data in zip(partition_results.keys(), computed_partitions):
        results[(calculation_name, partition_name)] = partition_data

    # Select final output series based on configuration
    final_outputs = []
    for output_name in self.config.output_series:
      output_data = results.get(output_name)
      if output_data is not None:
        final_outputs.append(output_data)
      else:
        raise ValueError(f"Output series '{output_name}' not found in results.")

    # Combine the selected outputs into a single DataFrame
    final_output = pd.concat(final_outputs, axis=0, ignore_index=True)

    return final_output