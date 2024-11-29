from dask.distributed import performance_report, Client
import pandas as pd

class DaskTelemetry:
  def __init__(self, client):
    self.client = client
    self.task_stream_data = []

  def start_task_stream(self):
    """Start collecting task stream data."""
    self.task_stream = self.client.get_task_stream()

  def stop_task_stream(self):
    """Stop collecting task stream data and store results."""
    self.task_stream_data.extend(self.task_stream.data)
    self.task_stream.close()

  def get_task_dataframe(self):
    """Convert task stream data into a DataFrame."""
    return pd.DataFrame(self.task_stream_data)
