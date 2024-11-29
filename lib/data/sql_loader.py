# sql_loader.py
import pandas as pd
from lib.data.base_loader import BaseDataLoader
import sqlalchemy

class SQLLoader(BaseDataLoader):
  def load_data(self) -> pd.DataFrame:
    """
    Load data from an SQL query.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    query = self.config.params["query"]
    connection_string = self.config.params["connection_string"]
    datetime_column = self.config.datetime_column

    # Connect to SQL database
    engine = sqlalchemy.create_engine(connection_string)
    data = pd.read_sql(query, engine)

    # Parse datetime column
    if datetime_column in data.columns:
      data[datetime_column] = pd.to_datetime(data[datetime_column])

    # Validate data
    return self.validate_data(data)
