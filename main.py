# main.py
import json
import argparse
from lib.config import PipelineConfig
from lib.pipeline.calculation_pipeline import CalculationPipeline

parser = argparse.ArgumentParser("Technically Systematic - Data Pipeline")
parser.add_argument("config_file",help="JSON config file")

if __name__ == '__main__':
  args = parser.parse_args()
  # Load configuration
  with open(args.config_file) as f:
      config_data = json.load(f)

  config = PipelineConfig(**config_data)

  # Run the pipeline
  pipeline = CalculationPipeline(config)
  results = pipeline.run()
  print(pipeline.manifest._output_filename)