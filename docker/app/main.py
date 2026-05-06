import os
import logging
import pandas as pd
from sqlalchemy import text

from app.db import get_engine, wait_for_db

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("etl")
csv_path = os.environ.get("CUSTOMER_CSV", "/data/input/customers.csv")

def split_name(full_name: str) -> tuple[str, str]:
    parts = str(full_name).strip().split()
    if not parts:
        return ("unknown", "unknown")
    if len(parts) == 1:
        return (parts[0], "unknown")
    return (parts[0], " ".join(parts[1:]))

def run():
    engine = get_engine()
    wait_for_db(engine)

    logger.info("Reading input CSV from %s", csv_path)
    df = pd.read_csv(csv_path)

    required_cols = {"customer_id", "full_name", "email"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    logger.info("Loading %s rows into staging_customers (replace load)", len(df))

    df.to_sql("staging_customers", engine, if_exists="replace", index=False)

    logger.info("Transforming staging -> dim_customers")
    dim = df.copy()
    dim[["first_name", "last_name"]] = dim["full_name"].apply(lambda x: pd.Series(split_name(x)))
    dim = dim[["customer_id", "first_name", "last_name", "email"]]

    upsert_sql = text("""
    INSERT INTO dim_customers (customer_id, first_name, last_name, email)
    VALUES (:customer_id, :first_name, :last_name, :email)
    ON CONFLICT (customer_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        email = EXCLUDED.email
    """)

    with engine.begin() as conn:
        for row in dim.to_dict(orient="records"):
            conn.execute(upsert_sql, row)

    logger.info("ETL process completed successfully. Verifying row counts.")
    with engine.connect() as conn:
        staging_count = conn.execute(text("SELECT COUNT(*) FROM staging_customers")).scalar()
        cur = conn.execute(text("SELECT COUNT(*) FROM dim_customers"))

    logger.info("Rows in staging_customers: %d", staging_count)
    logger.info("Rows in dim_customers: %d", cur.scalar())

if __name__ == "__main__":
    run()
