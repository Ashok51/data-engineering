from .config import Config
from .db import get_conn
from .pipeline import (
  finish_run,
  ensure_schema_and_tables,
  ingest_with_batching_and_quarentine,
  start_run,
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
            ensure_schema_and_tables(conn, cfg)  # This now commits internally
            conn.commit()  # Extra safety
            
            run_id = start_run(conn, cfg)
            conn.commit()  # Commit the run_id creation
            
            rows_read, rows_loaded, bad_rows = ingest_with_batching_and_quarentine(conn, cfg, run_id)
            
            transform_upsert_clean(conn, cfg, run_id)
            
            finish_run(conn, cfg, run_id, 'success', rows_read, rows_loaded, bad_rows, "OK")
            
            conn.commit()
            logger.info("Committed Successfully.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Pipeline Failed!! Error occurred: {e}")
            
            try:
                if run_id is not None:
                    finish_run(conn, cfg, run_id, 'failure', rows_read, rows_loaded, bad_rows, str(e))
                    conn.commit()
            except Exception as finish_error:
                logger.error(f"Failed to log failure: {finish_error}")
            raise