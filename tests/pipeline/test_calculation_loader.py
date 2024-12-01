from unittest.mock import patch, MagicMock
from lib.pipeline.calculation_loader import load_and_register_calculations
from lib.config import DataCalculationDefinition

@patch("importlib.import_module")
def test_load_and_register_calculations(mock_import_module):
  # Mock manifest
  manifest = [
    DataCalculationDefinition(
      module="lib.calculations.momentum",
      class_name="MomentumCalculation"
    ),
    DataCalculationDefinition(
      module="lib.calculations.reversion",
      class_name="ReversionCalculation"
    ),
  ]

  # Mock calculation classes
  mock_momentum_class = MagicMock(name="MomentumCalculation")
  mock_reversion_class = MagicMock(name="ReversionCalculation")

  # Mock modules returned by importlib
  mock_momentum_module = MagicMock()
  mock_reversion_module = MagicMock()
  setattr(mock_momentum_module, "MomentumCalculation", mock_momentum_class)
  setattr(mock_reversion_module, "ReversionCalculation", mock_reversion_class)

  mock_import_module.side_effect = lambda module: {
    "lib.calculations.momentum": mock_momentum_module,
    "lib.calculations.reversion": mock_reversion_module,
  }[module]

  # Execute the function
  result = load_and_register_calculations(manifest)

  # Assertions
  assert "MomentumCalculation" in result
  assert "ReversionCalculation" in result
  assert result["MomentumCalculation"] == mock_momentum_class
  assert result["ReversionCalculation"] == mock_reversion_class

  # Ensure importlib was called with the correct module names
  mock_import_module.assert_any_call("lib.calculations.momentum")
  mock_import_module.assert_any_call("lib.calculations.reversion")