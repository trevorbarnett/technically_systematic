from lib.output.base_output import Output
import pandas as pd

class CSVOutput(Output):
  def write(self, data: pd.DataFrame, **kwargs):
    """
    Write data to a CSV file.

    Args:
        data (pd.DataFrame): DataFrame to be written.
        **kwargs: Additional options for pd.to_csv.
    """
    file_path = self.config.params.get("file_path", "output.csv")
    data.to_csv(file_path, index=False, **kwargs)
    print(f"Data written to {file_path}")
