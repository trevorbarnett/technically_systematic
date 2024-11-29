# csv_loader.py
import pandas as pd
from lib.data.base_loader import BaseDataLoader

class CSVLoader(BaseDataLoader):
  def load_data(self) -> pd.DataFrame:
    """
    Load data from a CSV file.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    file_path = self.config.params["file_path"]
    datetime_column = self.config.datetime_column

    # Load CSV
    data = pd.read_csv(file_path, parse_dates=[datetime_column])

    # Validate data
    return self.validate_data(data)
