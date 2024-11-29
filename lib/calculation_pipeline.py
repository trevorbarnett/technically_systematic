from graphlib import TopologicalSorter
import importlib
import time
from typing import Union, List
from dask import delayed, compute
from dask.delayed import Delayed
from dask.distributed import Client, LocalCluster
from lib.calculation_loader import load_and_register_calculations
from lib.config import PipelineConfig, DaskScheduler, CacheConfig, OutputConfig
from lib.cache.base_cache import BaseCache
from lib.data_loader import create_loader
from lib.output.base_output import Output
from lib.manifest.output_manifest import OutputManifest
from lib.manifest.dask_telemtry import DaskTelemetry
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
    self.manifest = OutputManifest(config.pipeline_name, config.manifest_dir)

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
      results = {}
      # Start Dask task telemetry
      if self.dask_telemetry:
        self.dask_telemetry.start_task_stream()
      data = self.load_data()

      for calculation_name in self.sorted_calculations:
        self.manifest.start_job(calculation_name)

        # Find the corresponding calculation config
        calculation_config = next(s for s in self.config.calculations if s.output_name == calculation_name)
        calculation_class = self.calculation_classes[calculation_config.name]
        calculator = calculation_class(cache=self.cache)

        # Partition data as required by the calculation
        partitions = calculator.partition(data, **calculation_config.params)
        self.manifest.trigger_event(calculation_name,"partition_complete")
        try:
          partition_results = {}
          start_time = time.time()
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
                if isinstance(dep_data,tuple):
                  result,cache_file = dep_data
                else:
                  result = dep_data
                partition_data = partition_data.merge(result, on=["datetime", "asset"], how="left")

            # Create Dask task
            task = delayed(calculator.run)(
                partition_data, name=calculation_config.output_name, **calculation_config.params
            )
            partition_results[partition_name] = task

          # Merge partitions for the current calculation
          if self.config.dask.enabled:
            computed_partitions = compute(*partition_results.values())
          else:
            computed_partitions = [task.compute() for task in partition_results.values()]


          merged_results = []
          for task_result in computed_partitions:
            if isinstance(task_result,tuple):
              result, cache_path = task_result
              merged_results.append(result)
              if cache_path:
                self.manifest.add_cache_file(cache_path)
            else:
              merged_results.append(task_result)
          merged_results = pd.concat(merged_results, ignore_index=True)
          duration = time.time() - start_time

          # Log job timing
          self.manifest.job_timings[calculation_name] = {
            "start_time": start_time,
            "duration": duration
          }
          # Save the merged result for this calculation
          results[calculation_name] = merged_results

          # Also, store individual partitions in case downstream calculations need them
          for partition_name, partition_data in zip(partition_results.keys(), computed_partitions):
            results[(calculation_name, partition_name)] = partition_data

          self.manifest.end_job(calculation_name,success=True)
        except Exception as e:
          self.manifest.add_exception(f"Error in job '{calculation_name}': {e}",e) 
          self.manifest.end_job(calculation_name,success=False)
          raise
        finally:
          self.manifest.save()
      # Select final output series based on configuration
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
      # Stop Dask task telemetry
      if self.dask_telemetry:
        self.dask_telemetry.stop_task_stream()
        task_data = self.dask_telemetry.get_task_dataframe()
        self.manifest.add_log(f"Dask Task Telemetry: {task_data}")

      self.manifest.finalize("success")
    except Exception as e:
      self.manifest.add_exception(f"Pipeline failed: {e}", e)
      self.manifest.finalize("failure")
      raise
    finally:
      self.manifest.save()