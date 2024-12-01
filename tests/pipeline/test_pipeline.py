import pytest
from unittest.mock import patch, MagicMock
from pandas import DataFrame

from lib.pipeline.pipeline import CalculationPipeline
from lib.config import PipelineConfig, DataCalculationConfig, OutputConfig, CacheConfig, DaskScheduler, DataLoaderConfig, DataCalculationDefinition

@patch("lib.pipeline.pipeline.load_and_register_calculations")
@patch("lib.pipeline.pipeline.load_data")
@patch("lib.pipeline.pipeline.partition_data")
@patch("lib.pipeline.pipeline.execute_partitions")
@patch("lib.pipeline.pipeline.OutputManifest")
@patch("lib.manifest.dask_telemetry.DaskTelemetry")
@patch("dask.distributed.Client")
def test_calculation_pipeline_run(
    mock_dask_client,
    mock_dask_telemetry,
    mock_manifest,
    mock_execute_partitions,
    mock_partition_data,
    mock_load_data,
    mock_load_and_register_calculations,
):
  # Mock configurations
  mock_cache_config = CacheConfig(module="lib.cache.mock_cache", classname="MockCache")
  mock_output_config = [OutputConfig(module="lib.output.mock_output", class_name="MockOutput", data_series=["calc1"])]
  mock_calculation_config = [
      DataCalculationConfig(name="MockCalculation", output_name="calc1", dependencies=[])
  ]

  mock_data_loaders_config = {
      "price_series": DataLoaderConfig(
          module="lib.data.mock_price_series",
          class_name="MockPriceSeriesLoader"
      )
  }
  mock_calculations_manifest = [
    DataCalculationDefinition(
        module="lib.calculations.mock_calculator",
        class_name="MockCalculation",
    )
  ]

  pipeline_config = PipelineConfig(
    pipeline_name="TestPipeline",
    manifest_dir="test",
    cache=mock_cache_config,
    outputs=mock_output_config,
    calculations=mock_calculation_config,
    calculations_manifest=mock_calculations_manifest,
    data_loaders=mock_data_loaders_config,
    dask={"enabled": False, "scheduler": DaskScheduler.Threads}
  )

  # Mock dependencies
  mock_load_data.return_value = DataFrame({
    "datetime": ["2023-01-01", "2023-01-02"],
    "asset": ["AAPL", "GOOG"],
    "value": [150, 2800]
  })
  mock_partition_data.return_value = {
    "AAPL": DataFrame({"datetime": ["2023-01-01"], "asset": ["AAPL"], "value": [150]}),
    "GOOG": DataFrame({"datetime": ["2023-01-02"], "asset": ["GOOG"], "value": [2800]})
  }
  mock_execute_partitions.return_value = {
    "AAPL": DataFrame({"datetime": ["2023-01-01"], "asset": ["AAPL"], "calc1": [1.5]}),
    "GOOG": DataFrame({"datetime": ["2023-01-02"], "asset": ["GOOG"], "calc1": [2.8]})
  }

  mock_calculators = {"MockCalculation": MagicMock()}
  mock_load_and_register_calculations.return_value = mock_calculators

  mock_output = MagicMock()
  mock_output.write = MagicMock()
  class MockOutputConfig:
    data_series = ["calc1"]
  mock_output.config = MockOutputConfig()
  mock_manifest.return_value = MagicMock()

  # Instantiate the pipeline
  pipeline = CalculationPipeline(pipeline_config)

  # Replace outputs with mocked outputs
  pipeline.outputs = [mock_output]

  # Run the pipeline
  pipeline.run()


  # Verify dependencies
  mock_load_data.assert_called_once_with(pipeline.data_loaders)
  mock_partition_data.assert_called_once()
  mock_execute_partitions.assert_called_once()
  mock_output.write.assert_called_once()
  mock_manifest.return_value.finalize.assert_called_once_with("success")
