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
    'datetime': pd.date_range('2024-01-01', periods=20, freq='min'),
    'asset': ['AAPL'] * 10 + ['MSFT'] * 10,
    'open': [150, 151, 152, 153, 154, 155, 156, 157, 158, 159] * 2,
    'high': [151, 152, 153, 154, 155, 156, 157, 158, 159, 160] * 2,
    'low': [149, 150, 151, 152, 153, 154, 155, 156, 157, 158] * 2,
    'close': [150.5, 151.5, 152.5, 153.5, 154.5, 155.5, 156.5, 157.5, 158.5, 159.5] * 2,
    'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900] * 2
})

# Run the pipeline
pipeline = SignalPipeline(config)
results = pipeline.run(data)

print(results)
print(results.columns)
