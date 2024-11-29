# volatility.py
from lib.calculations.base_calculation import DataCalculation
import pandas as pd

class Volatility(DataCalculation):
 def calculate(self, data: pd.DataFrame, name: str, **kwargs) -> pd.DataFrame:
    """
        Calculate rolling volatility (standard deviation of returns).

        Args:
            data (pd.DataFrame): Input data.
            name (str): Name of the output column.
            **kwargs: Additional parameters, including:
                - column: The input column (default: "close").
                - window: The rolling window size.

        Returns:
            pd.DataFrame: DataFrame with the rolling volatility.
    """
    column = kwargs.get("column", "close")
    window = kwargs.get("window", 20)

    data = self.extract_columns(data, ["datetime", "asset", column])

        # Compute returns and rolling standard deviation
    returns = data[column].pct_change()
    data.loc[:, name] = returns.rolling(window=window).std()

    return data.loc[:, ["datetime", "asset", name]]