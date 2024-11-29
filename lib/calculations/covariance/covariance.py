# covariance.py
from lib.calculations.base_calculation import DataCalculation
import pandas as pd
from typing import Dict

class Covariance(DataCalculation):
  def partition(self, data: pd.DataFrame, **kwargs) -> Dict[str, pd.DataFrame]:
    """Covariance requires the entire universe, so return a single partition."""
    return {"universe": data}

  def calculate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
    Calculate a rolling covariance matrix for multiple assets.

    Args:
        data (pd.DataFrame): Input data.
        name (str): Name prefix for the output column.
        **kwargs: Additional parameters, including:
            - column: The input column (default: "close").
            - window: The rolling window size.

    Returns:
        pd.DataFrame: DataFrame with the rolling covariance matrix.
    """

    column = kwargs.get("column", "close")
    window = int(kwargs.get("window", 30))
    
    cov_asset_col = f"cov_asset_{column}_{window}"
    cov_value_col = f"cov_{column}_{window}"

    data = self.extract_columns(data,["datetime","asset",column])

    cov_matrix = data.pivot(index="datetime", columns="asset", values=column).rolling(window=window).cov()
    cov_matrix = cov_matrix.rename_axis(index={"asset": cov_asset_col})

    cov_matrix = (
        cov_matrix.stack(level=0)  # Stack asset_2 (columns) into a single level
        .reset_index()  # Flatten the index
    )
    # Rename columns for clarity
    cov_matrix.columns = ["datetime", "asset", cov_asset_col, cov_value_col]

    return cov_matrix