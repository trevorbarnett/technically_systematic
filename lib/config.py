from pydantic import BaseModel, Field, validator
from typing import List, Dict

class SignalDefinition(BaseModel):
  module: str # Path to the Python module containing the signal
  class_name: str # Class name of the signal generator

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
  signals_manifest: List[SignalDefinition] # List of signal defintions
  signals: List[SignalConfig] # List of signal configuration
  