# parquet_loader.py
import pandas as pd
from lib.data.base_loader import BaseDataLoader

class ParquetLoader(BaseDataLoader):
  def load_data(self) -> pd.DataFrame:
    """
    Load data from a Parquet file.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    file_path = self.config.params["file_path"]
    datetime_column = self.config.datetime_column

    # Load Parquet
    data = pd.read_parquet(file_path)

    # Parse datetime column
    if datetime_column in data.columns:
      data[datetime_column] = pd.to_datetime(data[datetime_column])

    # Validate data
    return self.validate_data(data)
