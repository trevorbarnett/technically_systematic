from lib.output.base_output import Output
import pandas as pd
import sqlalchemy

class SQLOutput(Output):
  def write(self, data: pd.DataFrame, **kwargs):
    """
    Write data to a SQL database.

    Args:
        data (pd.DataFrame): DataFrame to be written.
        **kwargs: Additional options for pd.to_sql.
    """
    connection_string = self.config.params.get("connection_string")
    table_name = self.config.params.get("table_name", "output_table")
    if not connection_string:
      raise ValueError("A connection string must be provided for SQL output.")

    engine = sqlalchemy.create_engine(connection_string)
    data.to_sql(table_name, engine, index=False, if_exists="replace", **kwargs)
    print(f"Data written to SQL table '{table_name}'")
