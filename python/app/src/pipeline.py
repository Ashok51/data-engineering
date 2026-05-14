import csv
from datetime import datetime
from psycopg import Connection
from .config import Config

def log(msg: str) -> None:

  # simple logging (we can later improve it)
  now =  datetime.now().isoformat(timespace="seconds")
  print(f"[{now}] {msg}", flush=True)

def ensure_schema_and_tables(conn: Connection, cfg: Config) -> None:
  log("Creating schema and tables if they don't exist... ")
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
        txn_day DATE NOT NULL
        );
      """)
    
def ingest_csv_to_raw(conn: Connection, cfg: Config) -> int:
  log(f"Reading CSV file from {cfg.csv_path}")
  rows = []
  with open(cfg.csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      rows.append(row)

  log(f"Loaded {len(rows)} rows from csv into memory.")

  with conn.cursor() as cur:
    for row in rows:
      cur.execute(f"""
                  INSERT INTO {cfg.schema}.raw_transactions
                  (txn_id, account_id, ts_event, amount, currency, channel)
                  values (%s, %s, %s, %s, %s, %s)
                  """,
                  (
                    row['txn_id'],
                    int(row['account_id']),
                    row['ts_event'],
                    float(row['amount']),
                    row['currency'],
                    row['channel']
                  )
      )

  log(f"Ingested {len(rows)} rows into raw_transactions table.")
  return len(rows)

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
