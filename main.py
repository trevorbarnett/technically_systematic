# main.py
import json
import pandas as pd
from lib.config import PipelineConfig
from lib.calculation_pipeline import CalculationPipeline

# Load configuration
with open("example_config.json") as f:
    config_data = json.load(f)

config = PipelineConfig(**config_data)

# Run the pipeline
pipeline = CalculationPipeline(config)
results = pipeline.run()

print(results)
