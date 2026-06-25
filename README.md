# Supply Chain Port Dwell Operations Dashboard

End-to-end supply chain analytics platform built on Snowflake, dbt, and Streamlit.

## Architecture
Raw Events → Snowflake RAW Layer → dbt Medallion Architecture (Bronze → Silver → Gold) → Streamlit Dashboard

## What It Does
- Ingests 51,517 container movement events for 10,000 containers
- Uses Snowflake MATCH_RECOGNIZE to identify port dwell bottlenecks
- Surfaces 801 containers exceeding the 5-day SLA
- Quantifies $1.1M in demurrage exposure across $168M in cargo value

## Tech Stack
- Snowflake, dbt, Python, Streamlit
