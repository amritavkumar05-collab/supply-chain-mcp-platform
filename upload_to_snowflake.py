#!/usr/bin/env python3
"""Upload local movement event Parquet data into Snowflake RAW layer."""

from __future__ import annotations

from pathlib import Path

import snowflake.connector
import toml

PROJECT_ROOT = Path(__file__).resolve().parent
SECRETS_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"
PARQUET_PATH = PROJECT_ROOT / "data" / "raw_movement_events.parquet"

DATABASE = "SUPPLY_CHAIN"
SCHEMA = "RAW"
TABLE = "MOVEMENT_EVENTS"
STAGE = "movement_stage"


def load_secrets() -> dict[str, str]:
    if not SECRETS_PATH.exists():
        raise FileNotFoundError(f"Missing secrets file: {SECRETS_PATH}")

    secrets = toml.load(SECRETS_PATH)
    required_keys = ("account", "user", "password", "warehouse")
    missing = [key for key in required_keys if not secrets.get(key)]
    if missing:
        raise ValueError(f"Missing required secrets keys: {', '.join(missing)}")

    return secrets


def execute_ddl(cursor: snowflake.connector.cursor.SnowflakeCursor, warehouse: str) -> None:
    cursor.execute(f"USE WAREHOUSE {warehouse}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}")
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {STAGE}")


def upload_parquet(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"Missing parquet file: {PARQUET_PATH}")

    parquet_uri = PARQUET_PATH.resolve().as_posix()
    cursor.execute(
        f"PUT file://{parquet_uri} @{STAGE} "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )


def create_movement_events_table(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{TABLE} (
            container_id VARCHAR(64),
            carrier_name VARCHAR(255),
            event_time TIMESTAMP_NTZ,
            event_type VARCHAR(64),
            estimated_value NUMBER(18, 2),
            origin_country VARCHAR(128)
        )
        """
    )


def load_parquet_into_table(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cursor.execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA}.{TABLE}")
    cursor.execute(
        f"""
        COPY INTO {DATABASE}.{SCHEMA}.{TABLE}
        FROM @{STAGE}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT'
        """
    )


def main() -> None:
    secrets = load_secrets()

    connection = snowflake.connector.connect(
        account=secrets["account"],
        user=secrets["user"],
        password=secrets["password"],
        role=secrets.get("role"),
        warehouse=secrets["warehouse"],
    )

    cursor = connection.cursor()
    try:
        print("Creating database, schema, and stage...")
        execute_ddl(cursor, secrets["warehouse"])

        print(f"Uploading {PARQUET_PATH.name} to @{STAGE}...")
        upload_parquet(cursor)

        print(f"Creating table {DATABASE}.{SCHEMA}.{TABLE}...")
        create_movement_events_table(cursor)

        print("Loading data with COPY INTO...")
        load_parquet_into_table(cursor)

        cursor.execute(f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{TABLE}")
        row_count = cursor.fetchone()[0]
        print(f"Success: loaded {row_count:,} rows into {DATABASE}.{SCHEMA}.{TABLE}.")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
