from lib.output.base_output import Output
import pandas as pd

class ParquetOutput(Output):
  def write(self, data: pd.DataFrame, **kwargs):
    """
    Write data to a Parquet file.

    Args:
        data (pd.DataFrame): DataFrame to be written.
        **kwargs: Additional options for pd.to_parquet.
    """
    file_path = self.config.params.get("file_path", "output.parquet")
    data.to_parquet(file_path, index=False, **kwargs)
    print(f"Data written to {file_path}")
