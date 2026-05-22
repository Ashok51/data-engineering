import csv
import json
import time
from typing import Iterator, Tuple, Dict, Any, List
from datetime import datetime
from psycopg.errors import OperationalError
from psycopg import Connection
from .config import Config
from .logger import setup_logger

logger = setup_logger()
ALLOWED_CHANNELS = {"web", "mobile", "pos"}
ALLOWED_CURRENCIES = {"NPR"}

def ensure_schema_and_tables(conn: Connection, cfg: Config) -> None:
  logger.info("Creating schema and tables if they do not exist...")
  with conn.cursor() as cur:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema}")

    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {cfg.schema}.raw_transactions (
        id BIGSERIAL PRIMARY KEY,
        txn_id TEXT NOT NULL,
        account_id INT NOT NULL,
        ts_event TIMESTAMPTZ NOT NULL,
        amount NUMERIC(12, 2) NOT NULL,
        currency TEXT NOT NULL,
        channel TEXT NOT NULL,
        run_id BIGINT NOT NULL REFERENCES {cfg.schema}.etl_runs(run_id),
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """)
    
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {cfg.schema}.clean_transactions (
        txn_id TEXT PRIMARY KEY,
        account_id INT NOT NULL,
        ts_event TIMESTAMPTZ NOT NULL,
        amount NUMERIC(12, 2) NOT NULL,
        currency TEXT NOT NULL,
        channel TEXT NOT NULL,
        txn_day DATE NOT NULL,
        last_run_id BIGINT NOT NULL REFERENCES {cfg.schema}.etl_runs(run_id)
        );
      """)

    # Log bad records into a separate table for later analysis
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {cfg.schema}.etl_runs (
        run_id BIGSERIAL PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ,
        status TEXT NOT NULL DEFAULT 'running',
        source_path TEXT NOT NULL,
        rows_read INT NOT NULL DEFAULT 0,
        rows_loaded INT NOT NULL DEFAULT 0,
        bad_rows INT NOT NULL DEFAULT 0,
        message TEXT
      );
      """)
    # Stores problem having records for later analysis.
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {cfg.schema}.bad_transactions ('
        id BIGSERIAL PRIMARY KEY,
        run_id BIGINT NOT NULL REFERENCES {cfg.schema}.etl_runs(run_id),
        raw_row JSONB NOT NULL,
        error TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

def start_run(conn: Connection, cfg: Config) -> int:
  logger.info("adding etl_run record")
  with conn.cursor() as cur:
    cur.execute(f"""
                INSERT INTO {cfg.schema}.etl_runs (source_path)
                VALUES (%s)
                RETURNING run_id;
                """, (cfg.csv_path,))
    run_id = cur.fetchone()[0]
  logger.info(f"Started ETL run with run_id={run_id}")
  
  return run_id
  
def finish_run(conn: Connection, cfg: Config, run_id: int, status: str, rows_read: int, rows_loaded: int, bad_rows: int, message: str = None) -> None:
  logger.info(f"finishing run_id={run_id} with status={status}")
  with conn.cursor() as cur:
    cur.execute(
                f"""
                UPDATE {cfg.schema}.etl_runs
                SET finished_at = NOW(),
                    status = %s,
                    rows_read = %s,
                    rows_loaded = %s,
                    bad_rows = %s,
                    message = %s
                WHERE run_id = %s;
                """,
                (status, rows_read, rows_loaded, bad_rows, message, run_id)
    )

def iter_csv_rows(cfg: Config) -> Iterator[Dict[str, str]]:
  with open(cfg.csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      yield row  # yielding one row at a time, return loads entire rows into memory, which is not ideal for large files

def validate_row(row: Dict[str, str]) -> Tuple[bool, Dict[str, Any] | None, str | None]:
  try:
    txn_id = row['txn_id'].strip()
    if not txn_id:
      return False, None, "txn_id is empty"
    
    account_id = int(row['account_id'])
    ts_event = row['ts_event'].strip()
    amount = float(row['amount'])
    currency = row['currency'].strip().upper()
    channel = row['channel'].strip().lower()

    if amount <= 0:
      return False, None, "amount must be > 0"

    if channel not in ALLOWED_CHANNELS:
      return False, None, f"Invalid channel: {channel}"
    
    if currency not in ALLOWED_CURRENCIES:
      return False, None, f"Invalid currency: {currency}"

    cleaned = {
      'txn_id': txn_id,
      'account_id': account_id,
      'ts_event': ts_event,
      'amount': amount,
      'currency': currency,
      'channel': channel
    }
    return True, cleaned, None
  except Exception as e:
    return False, None, f"validation error: {str(e)}"
  
def insert_bad_row(conn: Connection, cfg: Config, run_id: int, row: Dict[str, str], error: str) -> None:
  with conn.cursor() as cur:
    cur.execute(
      f"""
      INSERT INTO {cfg.schema}.bad_transactions (run_id, raw_row, error)
      VALUES (%s, %s::jsonb, %s);
      """,
      (run_id, json.dumps(row), error)
    )

def insert_raw_batch(conn: Connection, cfg: Config, run_id: int, batch: List[Dict[str, Any]]) -> None:
  if not batch:
    return 0
  
  values = [
    (
      row['txn_id'],
      row['account_id'],
      row['ts_event'],
      row['amount'],
      row['currency'],
      row['channel'],
      run_id
    )
    for row in batch
  ]

  with conn.cursor() as cur:
    cur.executemany(
      f"""
      INSERT INTO {cfg.schema}.raw_transactions
      (txn_id, account_id, ts_event, amount, currency, channel, run_id)
      VALUES (%s, %s, %s, %s, %s, %s, %s)
      ON CONFLICT (run_id, txn_id) DO NOTHING; -- avoid duplicates if retrying the same batch
      """,
      values
    )
  return len(batch)

def ingest_with_batching_and_quarentine(conn: Connection, cfg: Config, run_id: int) -> Tuple[int, int, int]:
  logger.info(f"Starting ingestion with batch_size={cfg.batch_size}")
  rows_read = 0
  rows_loaded = 0
  bad_rows = 0
  batch: List[Dict[str, Any]] = []

  for row in iter_csv_rows(cfg):
    rows_read += 1
    ok, cleaned, err = validate_row(row)

    if not ok:
      bad_rows += 1
      insert_bad_row(conn, cfg, run_id, row, err or "unknown error")
      continue

    batch.append(cleaned)

    if len(batch) >= cfg.batch_size:
      rows_loaded += insert_raw_batch_with_retries(conn, cfg, run_id, batch)
      batch.clear()  # clear the batch after inserting

  if batch:
    rows_loaded += insert_raw_batch_with_retries(conn, cfg, run_id, batch)
    batch.clear()

  logger.info(f"Finished ingestion: rows_read={rows_read}, rows_loaded={rows_loaded}, bad_rows={bad_rows}")
  return rows_read, rows_loaded, bad_rows


def insert_raw_batch_with_retries(conn: Connection, cfg: Config, run_id: int, batch: List[Dict[str, Any]]) -> int:
  attempt = 0
  while True:
    try:
      return insert_raw_batch(conn, cfg, run_id, batch)
    except OperationalError as e:
      attempt += 1
      if attempt > cfg.max_retries:
        logger.error(f"Failed to insert batch after {attempt} attempts: {str(e)}")
        raise
      sleep_for = cfg.retry_backoff_seconds * attempt
      logger.warning(f"Batch insert failed: {e}. Retrying in {sleep_for} seconds")
      time.sleep(sleep_for)

def transform_raw_to_clean(conn: Connection, cfg: Config) -> int:
  log("Transforming raw_transactions to clean_transactions...(upsert by txn_id to avoid duplicates)")

  with conn.cursor() as cur:
    cur.execute(
      f"""
      INSERT INTO {cfg.schema}.clean_transactions
      (txn_id, account_id, ts_event, amount, currency, channel, txn_day)
      SELECT
        txn_id,
        account_id,
        ts_event,
        amount,
        currency,
        channel,
        Date(ts_event) as txn_day
      FROM {cfg.schema}.raw_transactions
      ORDER BY id
      ON CONFLICT (txn_id)
      DO UPDATE SET
        account_id = EXCLUDED.account_id,
        ts_event = EXCLUDED.ts_event,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        channel = EXCLUDED.channel,
        txn_day = EXCLUDED.txn_day
        ;
      """)
    cur.execute(f"SELECT COUNT(*) FROM {cfg.schema}.clean_transactions;")
    count = cur.fetchone()[0]

  log(f"clean_transactions table have {count} rows.")
  return int(count)

def preview_results(conn: Connection, cfg: Config) -> None:
  log("Previewing some rows from clean_transactions:")
  with conn.cursor() as cur:
    cur.execute(f"""
                SELECT txn_id, account_id, ts_event, amount, currency, channel, txn_day
                FROM {cfg.schema}.clean_transactions
                LIMIT 5;
                """)
    rows = cur.fetchall()
    for row in rows:
      log(f"  {row}")
