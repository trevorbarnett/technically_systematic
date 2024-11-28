from enum import Enum
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional

class DaskScheduler(str, Enum):
  Threads = "threads"
  Processes = "processes"
  Distributed = "distributed"


class DaskConfig(BaseModel):
  enabled: bool = False # Whether to use Dask
  scheduler: DaskScheduler = DaskScheduler.Threads # Dask scheduler
  num_workers: Optional[int] = None # Number of workers (only applicable for "threads" or "processes")


class SignalDefinition(BaseModel):
  module: str # Path to the Python module containing the signal
  class_name: str # Class name of the signal generator

class CacheConfig(BaseModel):
  module: str
  classname: str
  params: Dict[str, str] = Field(default_factory=dict)

class SignalConfig(BaseModel):
  name: str # Signal name (e.g. 'momentum', 'reversion')
  output_name: str # Custon name for the output signal
  params: Dict[str, float] = Field(default_factory=dict)
  dependencies: List[str] = Field(default_factory=list)

  @validator("dependencies", pre=True)
  def validate_dependencies(cls, deps):
    if not isinstance(deps, list):
      raise ValueError("Dependencies must be a list of signal output names.")
    return deps
  

class PipelineConfig(BaseModel):
  dask: DaskConfig = DaskConfig()
  cache: Optional[CacheConfig] = None
  signals_manifest: List[SignalDefinition] # List of signal defintions
  signals: List[SignalConfig] # List of signal configuration
  output_series: List[str] # List of seires to output