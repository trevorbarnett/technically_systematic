import pandas as pd
from unittest.mock import MagicMock
from dask.delayed import Delayed
from lib.pipeline.executor import merge_dependencies, resolve_dependencies, execute_partitions

def test_merge_dependencies():
  # Partition data
  partition_data = pd.DataFrame({
    "datetime": ["2023-01-01", "2023-01-02"],
    "asset": ["AAPL", "GOOG"],
    "value": [150, 2800]
  })

  # Dependencies
  dependencies = {
    "volatility": pd.DataFrame({
      "datetime": ["2023-01-01", "2023-01-02"],
      "asset": ["AAPL", "GOOG"],
      "volatility": [0.05, 0.03]
    }),
    "sector": pd.DataFrame({
      "datetime": ["2023-01-01", "2023-01-02"],
      "asset": ["AAPL", "GOOG"],
      "sector": ["Tech", "Tech"]
    })
  }

  # Merge dependencies into partition data
  result = merge_dependencies(partition_data, dependencies)

  # Assertions
  assert not result.empty
  assert list(result.columns) == ["datetime", "asset", "value", "volatility", "sector"]
  assert result["volatility"].iloc[0] == 0.05
  assert result["sector"].iloc[1] == "Tech"

def test_resolve_dependencies():
  # Mock dependencies and results
  delayed_dependency = MagicMock(spec=Delayed)
  delayed_dependency.compute.return_value = pd.DataFrame({
    "datetime": ["2023-01-01"],
    "asset": ["AAPL"],
    "volatility": [0.05]
  })

  results = {
    ("volatility", "AAPL"): delayed_dependency,
    ("sector", "AAPL"): pd.DataFrame({
      "datetime": ["2023-01-01"],
      "asset": ["AAPL"],
      "sector": ["Tech"]
    })
  }

  # Mock calculation config
  calculation_config = MagicMock()
  calculation_config.dependencies = ["volatility", "sector"]

  # Resolve dependencies
  resolved = resolve_dependencies("AAPL", calculation_config, results)

  # Assertions
  assert "volatility" in resolved
  assert "sector" in resolved
  assert isinstance(resolved["volatility"], pd.DataFrame)
  assert isinstance(resolved["sector"], pd.DataFrame)

def test_execute_partitions():
  # Mock calculator
  mock_calculator = MagicMock()
  mock_calculator.run.return_value = pd.DataFrame({
      "datetime": ["2023-01-01", "2023-01-02"],
      "asset": ["AAPL", "AAPL"],
      "momentum": [1.2, 1.5]
  })

  # Mock partitions
  partitions = {
    "AAPL": pd.DataFrame({
      "datetime": ["2023-01-01", "2023-01-02"],
      "asset": ["AAPL", "AAPL"],
      "close": [150, 152]
    }),
    "GOOG": pd.DataFrame({
      "datetime": ["2023-01-01", "2023-01-02"],
      "asset": ["GOOG", "GOOG"],
      "close": [2800, 2850]
    }),
  }

  # Mock calculation config
  calculation_config = MagicMock()
  calculation_config.params = {"window": 5}
  calculation_config.output_name = "momentum"
  calculation_config.dependencies = []

  # Mock results dictionary (empty for this test)
  results = {}

  # Run execute_partitions
  with MagicMock() as mock_delayed:
    mock_delayed.return_value = mock_calculator.run  # Mock delayed behavior
    result = execute_partitions(mock_calculator, partitions, calculation_config, results)

  # Assertions
  assert "AAPL" in result
  assert "GOOG" in result
  assert result["AAPL"].shape[0] == 2
  assert list(result["AAPL"].columns) == ["datetime", "asset", "momentum"]
