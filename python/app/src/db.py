from contextlib import contextmanager
import psycopg
from psycopg import connection
from .config import Config

@contextmanager
def get_conn(cfg: Config) -> Connection:
  conn = psycopg.connect("".join(cfg.dsn()))
  try:
    yield conn # return directly returns so that there is not sure to run close(), but yield has ability to return and pause. 
  finally:
    conn.close()

