from graphlib import TopologicalSorter
import importlib
from typing import Union, List, Dict, Tuple
from dask.distributed import Client, LocalCluster
from lib.calculations.base_calculation import DataCalculation
from lib.pipeline.calculation_loader import load_and_register_calculations
from lib.config import PipelineConfig, DaskScheduler, CacheConfig, OutputConfig, LoggingLevel, DataCalculationConfig
from lib.cache.base_cache import BaseCache
from lib.pipeline.data_loader import create_loader
from lib.output.base_output import Output
from lib.manifest.output_manifest import OutputManifest
from lib.manifest.dask_telemetry import DaskTelemetry

from lib.pipeline.executor import execute_partitions
from lib.pipeline.partitioner import partition_data

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
    self.dask_telemetry = DaskTelemetry(self.dask_client) if self.dask_client else None
    self.outputs = self._initialize_outputs(config.outputs)
    self.manifest = OutputManifest(config.pipeline_name, config.manifest_dir, config.default_log_level)

  def _initialize_cache(self,cache_config: Union[CacheConfig, None]) -> Union[BaseCache]:
    if cache_config == None:
      return None
    cache_module_name = cache_config.module
    cache_classname = cache_config.classname
    cache_module = importlib.import_module(cache_module_name)
    cache_class = getattr(cache_module, cache_classname)
    return cache_class(cache_config)
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
  def _initialize_outputs(self,outputs: List[OutputConfig]) -> List[Output]:
    ret = []
    for output in outputs:
      output_module = importlib.import_module(output.module)
      output_class = getattr(output_module, output.class_name)
      ret.append(output_class(output))
    return ret

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
      if self.config.dask.remote_scheduler:
        scheduler_address = self.config.dask.remote_scheduler
        tls_config = {}
        if self.config.dask.remote_ssl:
          tls_config['ssl_ca_file'] = self.config.dask.remote_ssl.ca_file
          tls_config['ssl_cert'] = self.config.dask.remote_ssl.cert
          tls_config['ssl_key'] = self.config.dask.remote_ssl.key
        self.manifest.add_log(f"Connecting to remote Dask scheduler at {scheduler_address}")
        return Client(scheduler_address, **tls_config)
      else:
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
    try:
      # Start Dask task telemetry
      if self.dask_telemetry:
        self.dask_telemetry.start_task_stream()
      data = self.load_data()

      results = {}
      for calculation_name in self.sorted_calculations:
        self.process_calculation(calculation_name, data, results)
      final_output = self.generate_output(results)
      
      if self.dask_telemetry:
        self.log_dask_telemetry()
      
      self.manifest.finalize('success')
      return final_output
    except Exception as e:
      self.manifest.add_log(f"Pipeline failed: {e}", level = LoggingLevel.ERROR, exception = e)
      self.manifest.finalize("failure")
      raise
    finally:
      self.manifest.save() 
  def process_calculation(self, calculation_name: str, data: pd.DataFrame, results: Dict[str,pd.DataFrame]):
    """Process a single calculation: partion, resolves dependencies, and execute.

    Args:
        calculation_name (str): Name of the calculation
        data (pd.DataFrame): Data to operate upon
        results (dict): Results
    """
    
    self.manifest.start_job(calculation_name)
    calculation_config = self.get_calculation_config(calculation_name)
    calculator = self.get_calculator(calculation_config)

    # Partition data as required by the calculation
    partitions = partition_data(calculator, data, calculation_config)
    self.manifest.trigger_event(calculation_name,"partition_complete")

    try:
      partition_results = execute_partitions(calculator, partitions, calculation_config, results)
      merged_results = self.merge_results(partition_results)
      results[calculation_name] = merged_results
      self.manifest.end_job(calculation_name,success=True)
    except Exception as e:
      self.manifest.add_log(f"Error in job '{calculation_name}': {e}",level = LoggingLevel.ERROR, exception = e) 
      self.manifest.end_job(calculation_name,success=False)
      raise
    finally:
      self.manifest.save()
  def get_calculation_config(self, calculation_name: str) -> DataCalculationConfig:
    """Retrieve the configuration for a specific calculation

    Args:
        calculation_name (str): The calculation name
    """
    return next(s for s in self.config.calculations if s.output_name == calculation_name)
  
  def get_calculator(self, calculation_config: DataCalculationConfig) -> DataCalculation:
    """Instantiate the calculator class for a given calculation

    Args:
        calculation_config (DataCalculationConfig): Calculator config

    Raises:
        ValueError: If the calculator cannot be found based on the config

    Returns:
        DataCalculation: The instantiated calculator
    """
    calculation_class = self.calculation_classes[calculation_config.name]
    return calculation_class(cache=self.cache)
  
  def merge_results(self, partition_results: Dict[str,Union[Tuple[pd.DataFrame,str],pd.DataFrame]]) -> pd.DataFrame:
    """Merge partition results into a single DataFrame

    Args:
        partition_results (Dict[str, pd.DataFrame]): Partition calculation results

    Returns:
        pd.DataFrame: Merged results
    """
    merged_results = []
    for task_result in partition_results.values():
      if isinstance(task_result,tuple):
        result, cache_path = task_result
        merged_results.append(result)
        if cache_path:
          self.manifest.add_cache_file(cache_path)
      else:
        merged_results.append(task_result)
    return pd.concat(merged_results, ignore_index=True)
  
  def generate_output(self, results: Dict[str, pd.DataFrame]):
    """Generate final outputs and write them to configured destinations.

    Args:
        results (Dict[str, pd.DataFrame]): Results

    Raises:
        ValueError: Raised if the output series/columns can't be found in the results
    """
    for output in self.outputs:
      final_outputs = []
      for output_name in output.config.data_series:
        output_data = results.get(output_name)
        if output_data is not None:
          final_outputs.append(output_data)
        else:
          raise ValueError(f"Output series '{output_name}' not found in results.")

        # Combine the selected outputs into a single DataFrame
        final_output = pd.concat(final_outputs, axis=0, ignore_index=True)
        output.write(final_output)

  def log_dask_telemetry(self):
    """
    Log telemetry data from the Dask execution environment.
    """
    if not self.dask_telemetry:
      return

    # Stop telemetry collection
    self.dask_telemetry.stop_task_stream()

    # Retrieve telemetry data
    task_data = self.dask_telemetry.get_task_dataframe()

    if not task_data.empty:
      telemetry_summary = {
        "total_tasks": len(task_data),
        "total_runtime": task_data["duration"].sum() if "duration" in task_data.columns else None,
        "tasks": task_data.to_dict(orient="records")
      }

      self.manifest.add_log(f"Dask telemtry summary: {telemetry_summary}")
      self.manifest.dask_telemtry = telemetry_summary
    else:
      self.manifest.add_log("No Dask telemtry data available")
