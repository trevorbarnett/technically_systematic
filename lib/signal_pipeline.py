from graphlib import TopologicalSorter
from dask import delayed, compute
from dask.delayed import Delayed
from dask.distributed import Client, LocalCluster
from lib.signal_loader import load_and_register_signals
from lib.config import PipelineConfig, DaskScheduler
import pandas as pd

class SignalPipeline:
  def __init__(self, config: PipelineConfig):
    """Initialize the signal pipeline with a configuration

    Args:
        config (PipelineConfig): Configuration defining which signals to load generate and their parameters
    """
    self.config = config
    self.sorted_signals = self._sort_signals_by_dependencies()
    self.signal_classes = load_and_register_signals(config.signals_manifest)
    self.dask_client = self._setup_dask() if config.dask.enabled else None

  def _sort_signals_by_dependencies(self):
    dag = TopologicalSorter()
    for signal in self.config.signals:
      dag.add(signal.output_name, *signal.dependencies)
  
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
    

  def run(self, data: pd.DataFrame) -> pd.DataFrame:
    results = {}
    tasks = {}
    for signal_name in self.sorted_signals:
      # Find the corresponding signal config
      signal_config = next(s for s in self.config.signals if s.output_name == signal_name)
      signal_class = self.signal_classes[signal_config.name]
      generator = signal_class()
      
      # Partition data as required by the signal
      partitions = generator.partition(data, **signal_config.params)

      for partition_name, partition_data in partitions.items():
        dependencies = {}
        for dep_name in signal_config.dependencies:
          dep_key = (dep_name, partition_name)
          dep_result = results.get(dep_key)
          if isinstance(dep_result, Delayed):
            dependencies[dep_name] = dep_result.compute()
          else:
            dependencies[dep_name] = dep_result
  
        for dep_name, dep_data in dependencies.items():
          if dep_data is not None:
            partition_data = partition_data.merge(dep_data, on=["datetime", "asset"], how="left")
        # Create dask task
        task = delayed(generator.generate)(
          partition_data, name=signal_config.output_name, **signal_config.params
        )
        
        tasks[(signal_name, partition_name)] = task
      results.update(tasks)

    if self.config.dask.enabled:
      # Compute all tasks in parallel
      computed_tasks = compute(**tasks.values())
      results = dict(zip(tasks.keys(), computed_tasks))
    else:
      results = {key: task.compute() for key, task in tasks.items()}

    return results