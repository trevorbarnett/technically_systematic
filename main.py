# main.py
import json
import pandas as pd
from lib.config import PipelineConfig
from lib.signal_loader import load_and_register_signals
from lib.signal_pipeline import SignalPipeline

# Load configuration
with open("example_config.json") as f:
    config_data = json.load(f)

config = PipelineConfig(**config_data)

# Run the pipeline
pipeline = SignalPipeline(config)
results = pipeline.run()

print(results)
