from graphlib import TopologicalSorter
from lib.signal_loader import load_and_register_signals
from lib.config import PipelineConfig
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
  
  def _sort_signals_by_dependencies(self):
    dag = TopologicalSorter()
    for signal in self.config.signals:
      dag.add(signal.output_name, *signal.dependencies)
  
    return list(dag.static_order()) 
  
  def run(self, data: pd.DataFrame) -> pd.DataFrame:
    results = {}
    for signal_name in self.sorted_signals:
      # Find the corresponding signal config
      signal_config = next(s for s in self.config.signals if s.output_name == signal_name)
      signal_class = self.signal_classes[signal_config.name]
      generator = signal_class()
      
      # Get dependencies
      dependencies = {dep: results[dep] for dep in signal_config.dependencies}
      result = generator.generate(data, name=signal_config.output_name, **signal_config.params, **dependencies)
      results[signal_name] = result
    return results