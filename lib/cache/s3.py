# s3_cache.py
import pandas as pd
import boto3
from io import BytesIO
from lib.cache.base_cache import BaseCache

class S3Cache(BaseCache):
  def __init__(self, bucket_name, aws_access_key, aws_secret_key):
    self.s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    self.bucket_name = bucket_name

  def save(self, key: str, data: pd.DataFrame):
    buffer = BytesIO()
    data.to_pickle(buffer)
    buffer.seek(0)
    self.s3.upload_fileobj(buffer, self.bucket_name, f"{key}.pkl")

  def load(self, key: str) -> pd.DataFrame:
    buffer = BytesIO()
    self.s3.download_fileobj(self.bucket_name, f"{key}.pkl", buffer)
    buffer.seek(0)
    return pd.read_pickle(buffer)

  def exists(self, key: str) -> bool:
    try:
      self.s3.head_object(Bucket=self.bucket_name, Key=f"{key}.pkl")
      return True
    except:
      return False
