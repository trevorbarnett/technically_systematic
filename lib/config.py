from enum import Enum
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Optional
import pandas as pd

class LoggingLevel(str, Enum):
  NOTSET = ""
  DEBUG = "DEBUG"
  INFO = "INFO"
  WARNING = "WARNING"
  ERROR = "ERROR"
  CRITICAL = "CRITICAL"

class DaskScheduler(str, Enum):
  Threads = "threads"
  Processes = "processes"
  Distributed = "distributed"

class DaskSSLConfig(BaseModel):
  ca_file: str
  cert: str
  key: str

class DaskConfig(BaseModel):
  enabled: bool = False # Whether to use Dask
  scheduler: DaskScheduler = DaskScheduler.Threads # Dask scheduler
  num_workers: Optional[int] = None # Number of workers (only applicable for "threads" or "processes")
  remote_scheduler: Optional[str] = None # Remote cluster address
  remote_ssl: Optional[DaskSSLConfig] = None

class DataJoinType(str, Enum):
  Left = "left"
  Right = "right"
  Inner = "inner"
  Outer = "outer"

class DataFillType(str, Enum):
  ForwardFill = "ffill"
  BackwardFill = "bfill"

class DataAssociation(BaseModel):
  on: List[str]
  how: DataJoinType
  fill_method: Optional[DataFillType] = None

class DataLoaderConfig(BaseModel):
  module: str
  class_name: str
  datetime_column: str = "datetime"
  required_columns: List[str] = ["datetime"]
  datetime_granularity: Optional[str] = None
  association: Optional[DataAssociation] = None
  params: Dict[str,str] = {}

  @field_validator("datetime_granularity")
  def validate_dt_granularity(cls, granularity):
    try:
      pd.to_timedelta(granularity)
    except ValueError:
      raise ValueError(f"Invalid granularity for DataLoader: {granularity}")
    return granularity

class OutputConfig(BaseModel):
  module: str
  class_name: str
  data_series: List[str]
  params: Dict[str,str] = {}

class DataCalculationDefinition(BaseModel):
  module: str # Path to the Python module containing the signal
  class_name: str # Class name of the signal generator

class CacheConfig(BaseModel):
  module: str
  classname: str
  params: Dict[str, str] = Field(default_factory=dict)

class DataCalculationConfig(BaseModel):
  name: str # Calculation name (e.g. 'momentum', 'reversion')
  output_name: str # Custon name for the output signal
  params: Dict[str, float] = Field(default_factory=dict)
  dependencies: List[str] = Field(default_factory=list)

  @field_validator("dependencies")
  def validate_dependencies(cls, deps):
    if not isinstance(deps, list):
      raise ValueError("Dependencies must be a list of signal output names.")
    return deps
  

class PipelineConfig(BaseModel):
  pipeline_name: str
  manifest_dir: str
  default_log_level: LoggingLevel = LoggingLevel.INFO
  dask: DaskConfig = DaskConfig()
  cache: Optional[CacheConfig] = None
  data_loaders: Dict[str, DataLoaderConfig]
  calculations_manifest: List[DataCalculationDefinition] # List of signal defintions
  calculations: List[DataCalculationConfig] # List of signal configuration
  outputs: List[OutputConfig] = []