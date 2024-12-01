from unittest.mock import MagicMock
import pandas as pd
from lib.pipeline.partitioner import partition_data
from lib.calculations.base_calculation import DataCalculation
from lib.config import DataCalculationConfig

def test_partition_data():
  # Mock calculator
  mock_calculator = MagicMock(spec=DataCalculation)
  mock_calculator.partition.return_value = {
    "AAPL": pd.DataFrame({
      "datetime": ["2023-01-01", "2023-01-02"],
      "asset": ["AAPL", "AAPL"],
      "value": [150, 152]
    }),
    "GOOG": pd.DataFrame({
      "datetime": ["2023-01-01", "2023-01-02"],
      "asset": ["GOOG", "GOOG"],
      "value": [2800, 2850]
    }),
  }

  # Input data
  data = pd.DataFrame({
    "datetime": ["2023-01-01", "2023-01-02", "2023-01-01", "2023-01-02"],
    "asset": ["AAPL", "AAPL", "GOOG", "GOOG"],
    "value": [150, 152, 2800, 2850]
  })

  # Mock calculation config
  calculation_config = MagicMock(spec=DataCalculationConfig)
  calculation_config.params = {"some_param": "value"}

  # Run partition_data function
  result = partition_data(mock_calculator, data, calculation_config)

  # Assertions
  mock_calculator.partition.assert_called_once_with(data, **calculation_config.params)
  assert "AAPL" in result
  assert "GOOG" in result
  assert result["AAPL"].shape[0] == 2
  assert result["GOOG"].shape[0] == 2
