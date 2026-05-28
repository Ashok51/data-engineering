from contextlib import contextmanager
import psycopg
from psycopg import connection
from psycopg.errors import OperationalError
from .logger import setup_logger
from .config import Config
import time

logger = setup_logger()

def connect_with_retries(cfg: Config) -> connection:
  attempt = 0
  while True:
    try:
      logger.info(f"Attempting to connect to the database. Attempt {attempt + 1}")
      conn = psycopg.connect("".join(cfg.dsn()))
      logger.info("Successfully connected to the database.")

      return conn
    except OperationalError as e:
      attempt += 1
      if attempt >= cfg.max_retries:
        logger.error(f"DB connection failed after {attempt} attempts with error: {e}")
        raise
      wait_time = cfg.retry_backoff_seconds * attempt
      logger.warning(f"DB connection failed: {e}. Retrying in {wait_time} seconds...")
      time.sleep(wait_time)

@contextmanager
def get_conn(cfg: Config) -> connection:
  conn = psycopg.connect("".join(cfg.dsn()))
  try:
    yield conn # return directly returns so that there is not sure to run close(), but yield has ability to return and pause. 
  finally:
    conn.close()

