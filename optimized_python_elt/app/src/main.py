from .config import Config
from .db import get_conn
from .pipeline import (
  finish_run,
  log,
  ensure_schema_and_tables,
  ingest_csv_to_raw,
  transform_raw_to_clean,
  preview_results,
  start_run,
  start_pipeline_run,
  transform_upsert_clean,
  setup_logger
)

logger = setup_logger()

def main():
    cfg = Config()
    with get_conn(cfg) as conn:
        run_id = None
        rows_read = 0
        rows_loaded = 0
        bad_rows = 0

        try:
            ensure_schema_and_tables(conn, cfg)
            run_id = start_run(conn, cfg)

            rows_read, rows_loaded, bad_rows = start_pipeline_run(conn, cfg, run_id)

            transform_upsert_clean(conn, cfg, run_id)

            finish_run(conn, cfg, run_id, 'success', rows_read, rows_loaded, bad_rows, "OK")

            conn.commit()
            log("Committed Successfully.")
        except Exception as e:
            conn.rollback()
            log(f"Pipeline Failed!! Error occurred: {e}")
            
            try:
                if run_id is not None:
                    finish_run(conn, cfg, run_id, 'failure', rows_read, rows_loaded, bad_rows, str(e))
                    conn.commit()
            except Exception:
                pass
            raise

if __name__ == "__main__":
    main()