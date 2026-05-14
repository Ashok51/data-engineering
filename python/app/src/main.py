from .config import Config
from .db import get_conn
from .pipeline import (
  log,
  ensure_schema_and_tables,
  ingest_csv_to_raw,
  transform_raw_to_clean,
  preview_results,
)

def main():
    cfg = Config()
    with get_conn(cfg) as conn:
        try:
            ensure_schema_and_tables(conn, cfg)
            ingested = ingest_csv_to_raw(conn, cfg)
            log(f"Ingested {ingested} rows into raw table.")

            clean_count = transform_raw_to_clean(conn, cfg)
            log(f"Transformed {clean_count} rows into clean table.")

            conn.commit()
            log("Committed Successfully.")
        except Exception as e:
            conn.rollback()
            log(f"Pipeline Failed!! Error occurred: {e}")
            raise
        preview_results(conn, cfg)
        log("Pipeline execution completed.")

if __name__ == "__main__":
    main()