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

# Example multi-asset data
data = pd.DataFrame({
    'asset': ['AAPL', 'AAPL', 'AAPL', 'MSFT', 'MSFT', 'MSFT','GOOG','GOOG','GOOG'],
    'price': [150, 151, 152, 300, 305, 310, 650, 625, 635],
    'datetime': pd.date_range('2024-01-01', periods=9, freq='min')
})

# Run the pipeline
pipeline = SignalPipeline(config)
results = pipeline.run(data)

print(results)
print(results.columns)
