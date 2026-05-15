import os

def env_str(key, default: str | None = None) -> str:
  val = os.getenv(key, default)
  if val is None or val.strip() == "":
    raise ValueError(f"Environment variable '{key}' is missing.")
  return val

def env_int(key, default: int | None = None) -> int:
  raw = os.getenv(key)
  if raw is None or raw.strip() == "":
    if default is not None:
      raise ValueError(f"Environment variable '{key}' is missing.")
    return default
  return int(raw)

class Config:
  def __init__(self):
    self.pg_host = env_str("POSTGRES_HOST")
    self.pg_port = env_int("POSTGRES_PORT")
    self.pg_user = env_str("POSTGRES_USER")
    self.pg_password = env_str("POSTGRES_PASSWORD")
    self.pg_db = env_str("POSTGRES_DB")
    self.schema =  env_str("APP_SCHEMA", "data_pipeline_example_2")
    self.csv_path = env_str("CSV_PATH", "data/transactions.csv")

    self.batch_size = env_int("BATCH_SIZE", 500)
    self.max_retries = env_int("MAX_RETRIES", 5)
    self.retry_backoff_seconds = env_int("RETRY_BACKOFF_SECONDS", 2)

  def dsn(self):
    return(
      f"host={self.pg_host} ",
      f"port={self.pg_port} ",
      f"dbname={self.pg_db} ",
      f"user={self.pg_user} ",
      f"password={self.pg_password}"
    )