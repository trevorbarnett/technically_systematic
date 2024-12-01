import pytest
import pandas as pd

from unittest.mock import patch, MagicMock
from lib.pipeline.data_loader import create_loader, load_data
from lib.data.mock_loader import MockLoader
from lib.config import DataLoaderConfig, DataJoinType, DataFillType

@patch("importlib.import_module")
def test_create_loader(mock_import_module):
    # Mock configuration for the loader
    loader_config = DataLoaderConfig(module="lib.data.mock_loader", class_name="MockLoader")
    mock_loader_class = MagicMock()
    class MockModule:
        def __init__(self):
            self.MockLoader = mock_loader_class
    mock_import_module.return_value = MockModule()
    mock_loader_instance = mock_loader_class.return_value

    # Test loader creation
    loader = create_loader(loader_config)

    # Assertions
    mock_import_module.assert_called_once_with("lib.data.mock_loader")
    assert loader == mock_loader_instance

def test_load_data_with_mock_loader():
    config = DataLoaderConfig(module="lib.data.mock_loader", class_name="MockLoader",
        association=dict(
            on=["asset", "datetime"],
            how=DataJoinType.Left,
            fill_method=DataFillType.ForwardFill
        )
    )
    # Setup mock loaders
    price_loader = MockLoader(config=config)
    secondary_loader = MockLoader(config=config)

    data_loaders = {
        "price_series": price_loader,
        "secondary_data": secondary_loader
    }

    # Run load_data function
    result = load_data(data_loaders)

    # Assertions
    assert not result.empty
    assert set(result.columns) == {"datetime", "asset", "value", "value_secondary"}
    assert result.shape[0] == 2  # Number of rows should match primary data

def test_associate_default():
    # Primary dataset
    primary = pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-02"],
        "asset": ["AAPL", "GOOG"],
        "price": [150, 2800]
    })

    # Secondary dataset
    secondary = pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-03"],
        "asset": ["AAPL", "GOOG"],
        "sector": ["Tech", "Tech"]
    })

    # Configuration for association
    config = DataLoaderConfig(module="lib.data.mock_loader", class_name="MockLoader",
        association=dict(
            on=["asset", "datetime"],
            how=DataJoinType.Left,
            fill_method=DataFillType.ForwardFill
        )
    )

    loader = MockLoader(config=config)
    result = loader.associate(primary, secondary)

    # Assertions
    assert not result.empty
    assert list(result.columns) == ["datetime", "asset", "price", "sector"]
    assert result["sector"].iloc[1] == "Tech"  # Forward fill applied

def test_associate_inner_join():
    # Primary dataset
    primary = pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-02"],
        "asset": ["AAPL", "GOOG"],
        "price": [150, 2800]
    })

    # Secondary dataset
    secondary = pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-03"],
        "asset": ["AAPL", "GOOG"],
        "sector": ["Tech", "Tech"]
    })

    # Configuration for association
    config = DataLoaderConfig(module="lib.data.mock_loader", class_name="MockLoader",
        association=dict(
            on=["asset", "datetime"],
            how=DataJoinType.Inner,
            fill_method=None
        )
    )

    loader = MockLoader(config=config)
    result = loader.associate(primary, secondary)

    # Assertions
    assert not result.empty
    assert list(result.columns) == ["datetime", "asset", "price", "sector"]
    assert len(result) == 1  # Only 1 row matches for an inner join

def test_associate_outer_join_no_fill():
    # Primary dataset
    primary = pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-02"],
        "asset": ["AAPL", "GOOG"],
        "price": [150, 2800]
    })

    # Secondary dataset
    secondary = pd.DataFrame({
        "datetime": ["2023-01-01", "2023-01-03"],
        "asset": ["AAPL", "GOOG"],
        "sector": ["Tech", "Tech"]
    })

    # Configuration for association
    config = DataLoaderConfig(module="lib.data.mock_loader", class_name="MockLoader",
        association=dict(
            on=["asset", "datetime"],
            how=DataJoinType.Outer,
            fill_method=None
        )
    )

    loader = MockLoader(config=config)
    result = loader.associate(primary, secondary)

    # Assertions
    assert not result.empty
    assert list(result.columns) == ["datetime", "asset", "price", "sector"]
    assert result["sector"].isna().sum() == 1  # No fill method applied
    assert result["price"].isna().sum() == 1
