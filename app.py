"""Operational supply chain dashboard for port dwell bottlenecks."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

import pandas as pd
import snowflake.connector
import streamlit as st

SLA_DAYS = 5
DEFAULT_DAILY_DEMURRAGE_RATE = 275.0
GOLD_TABLE = "MART_PORT_DWELL_BOTTLENECKS"
TOTAL_FLEET_SIZE = 10000

GOLD_QUERY = f"""
select
    coalesce(container_id::varchar, '') as container_id,
    coalesce(carrier_name::varchar, 'Unknown') as carrier_name,
    coalesce(origin_country::varchar, 'Unknown') as origin_country,
    cast(depart_origin_at as timestamp_ntz) as depart_origin_at,
    cast(arrive_port_at as timestamp_ntz) as arrive_port_at,
    cast(depart_port_at as timestamp_ntz) as depart_port_at,
    cast(coalesce(port_dwell_days, 0) as double) as port_dwell_days,
    cast(coalesce(intermediate_event_count, 0) as double) as intermediate_event_count,
    cast(coalesce(estimated_value_at_arrival, 0.00) as double) as estimated_value_at_arrival
from SUPPLY_CHAIN_DB.BRONZE_GOLD.{GOLD_TABLE}
order by port_dwell_days desc, arrive_port_at asc
"""

EMPTY_BOTTLENECK_COLUMNS = [
    "container_id",
    "carrier_name",
    "origin_country",
    "depart_origin_at",
    "arrive_port_at",
    "depart_port_at",
    "port_dwell_days",
    "intermediate_event_count",
    "estimated_value_at_arrival",
]


def _get_config_value(key: str, default: str | None = None) -> str | None:
    """Read configuration from Streamlit secrets first, then environment variables."""
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except (FileNotFoundError, AttributeError, KeyError):
        pass
    env_value = os.getenv(key.upper()) or os.getenv(f"SNOWFLAKE_{key.upper()}")
    if env_value:
        return env_value
    return default


def _build_snowflake_config() -> dict[str, Any]:
    config = {
        "account": _get_config_value("account"),
        "user": _get_config_value("user"),
        "password": _get_config_value("password"),
        "role": _get_config_value("role"),
        "warehouse": _get_config_value("warehouse"),
        "database": "SUPPLY_CHAIN_DB",
        "schema": "BRONZE_GOLD",
    }
    missing = [key for key in ("account", "user", "password", "warehouse") if not config[key]]
    if missing:
        raise ValueError(
            "Missing Snowflake connection settings. Set keys in `.streamlit/secrets.toml` "
            f"or environment variables: {', '.join(missing)}."
        )
    return config


@contextmanager
def snowflake_connection() -> Iterator[snowflake.connector.SnowflakeConnection]:
    config = _build_snowflake_config()
    connection = snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        role=config["role"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
    )
    try:
        yield connection
    finally:
        connection.close()


def _is_missing_table_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "does not exist" in message
        or ("object '" in message and "not found" in message)
        or "002043" in message
        or "02000" in message
    )


def _coerce_bottleneck_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    typed = df.copy()
    for column in ("port_dwell_days", "intermediate_event_count", "estimated_value_at_arrival"):
        typed[column] = pd.to_numeric(typed[column], errors="coerce").fillna(0)
    for column in ("depart_origin_at", "arrive_port_at", "depart_port_at"):
        typed[column] = pd.to_datetime(typed[column], errors="coerce", utc=False)
        if hasattr(typed[column].dt, "tz") and typed[column].dt.tz is not None:
            typed[column] = typed[column].dt.tz_localize(None)
    return typed


def _fetch_gold_dataframe(connection: snowflake.connector.SnowflakeConnection) -> pd.DataFrame:
    cursor = connection.cursor()
    try:
        cursor.execute(GOLD_QUERY)
        columns = [column[0].lower() for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        for col in ("depart_origin_at", "arrive_port_at", "depart_port_at"):
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: pd.Timestamp(x) if x is not None else pd.NaT
                )
        return _coerce_bottleneck_dtypes(df)
    finally:
        cursor.close()


@st.cache_data(show_spinner="Loading Gold layer data from Snowflake...")
def load_bottleneck_data() -> pd.DataFrame:
    try:
        with snowflake_connection() as connection:
            return _fetch_gold_dataframe(connection)
    except (
        snowflake.connector.errors.ProgrammingError,
        snowflake.connector.errors.DatabaseError,
    ) as exc:
        if _is_missing_table_error(exc):
            st.warning(
                f"The Gold table `{GOLD_TABLE}` has not been materialized yet. "
                "Run `dbt run` from the `dbt_supply_chain` project to build the Gold layer, "
                "then refresh this dashboard."
            )
            return pd.DataFrame(columns=EMPTY_BOTTLENECK_COLUMNS)
        raise


def enrich_action_list(df: pd.DataFrame, daily_demurrage_rate: float) -> pd.DataFrame:
    enriched = df.copy()
    enriched["days_over_sla"] = (enriched["port_dwell_days"] - SLA_DAYS).clip(lower=0)
    enriched["demurrage_at_risk_usd"] = (
        enriched["days_over_sla"] * daily_demurrage_rate
    ).round(2)
    enriched["severity"] = pd.cut(
        enriched["days_over_sla"],
        bins=[-1, 0, 5, 10, float("inf")],
        labels=["On Track", "Low", "Medium", "Critical"],
    )
    return enriched.sort_values(
        ["days_over_sla", "port_dwell_days", "arrive_port_at"],
        ascending=[False, False, True],
    )


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def render_metric_cards(df: pd.DataFrame) -> None:
    total_demurrage = df["demurrage_at_risk_usd"].sum() if not df.empty else 0.0
    average_dwell = df["port_dwell_days"].mean() if not df.empty else 0.0
    total_containers = len(df)
    total_cargo_value = df["estimated_value_at_arrival"].sum() if not df.empty else 0.0
    fleet_pct = (total_containers / TOTAL_FLEET_SIZE) * 100

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        label="Total Demurrage at Risk",
        value=format_currency(total_demurrage),
        help="Estimated demurrage exposure for days exceeding the 5-day SLA.",
    )
    col2.metric(
        label="Average Port Dwell (Days)",
        value=f"{float(average_dwell):,.0f}",
        help="Mean days between ARRIVE_PORT and DEPART_PORT for filtered containers.",
    )
    col3.metric(
        label="Bottlenecked Containers",
        value=f"{int(total_containers):,} ({fleet_pct:.1f}%)",
        help=f"Containers exceeding the 5-day SLA out of {TOTAL_FLEET_SIZE:,} total.",
    )
    col4.metric(
        label="Total Cargo Value at Risk",
        value=format_currency(total_cargo_value),
        help="Estimated total cargo value for all bottlenecked containers.",
    )


def render_carrier_chart(df: pd.DataFrame) -> None:
    st.subheader("Demurrage at Risk by Carrier")
    st.caption("Top 10 carriers ranked by total estimated demurrage exposure (USD).")

    if df.empty:
        st.info("No data available.")
        return

    carrier_summary = (
        df.groupby("carrier_name")["demurrage_at_risk_usd"]
        .sum()
        .reset_index()
        .sort_values("demurrage_at_risk_usd", ascending=True)
        .tail(10)
    )

    st.bar_chart(
        carrier_summary.set_index("carrier_name")["demurrage_at_risk_usd"],
        use_container_width=True,
        color="#FF4B4B",
        horizontal=True,
    )


def render_origin_chart(df: pd.DataFrame) -> None:
    st.subheader("Bottlenecked Containers by Origin Country")
    st.caption("Top 10 origin countries by number of delayed containers.")

    if df.empty:
        st.info("No data available.")
        return

    origin_summary = (
        df.groupby("origin_country")["container_id"]
        .count()
        .reset_index()
        .rename(columns={"container_id": "container_count"})
        .sort_values("container_count", ascending=True)
        .tail(10)
    )

    st.bar_chart(
        origin_summary.set_index("origin_country")["container_count"],
        use_container_width=True,
        color="#FF6B35",
        horizontal=True,
    )

    st.bar_chart(
        origin_summary.set_index("origin_country")["container_count"],
        use_container_width=True,
        color="#FF6B35",
    )


def render_dwell_distribution(df: pd.DataFrame) -> None:
    st.subheader("Port Dwell Time Distribution")
    st.caption("Number of containers by dwell time bucket (days over SLA).")

    if df.empty:
        st.info("No data available.")
        return

    bins = [0, 3, 6, 9, 12, float("inf")]
    labels = ["1-3 days", "4-6 days", "7-9 days", "10-12 days", "13+ days"]
    bucketed = pd.cut(
        df["days_over_sla"],
        bins=bins,
        labels=labels,
        right=True,
    ).value_counts().reindex(labels).fillna(0).reset_index()
    bucketed.columns = ["Dwell Bucket", "Container Count"]

    st.bar_chart(
        bucketed.set_index("Dwell Bucket")["Container Count"],
        use_container_width=True,
        color="#9B59B6",
    )


def render_action_list(df: pd.DataFrame) -> None:
    st.subheader("At-Risk Action List")
    st.caption(
        "Delayed shipments crossing the 5-day SLA threshold. Prioritize containers "
        "with the highest days over SLA and demurrage exposure."
    )

    if df.empty:
        st.info("No bottlenecked containers match the current filters.")
        return

    display_columns = [
        "container_id",
        "carrier_name",
        "origin_country",
        "severity",
        "arrive_port_at",
        "depart_port_at",
        "port_dwell_days",
        "days_over_sla",
        "estimated_value_at_arrival",
        "demurrage_at_risk_usd",
        "intermediate_event_count",
    ]

    column_config = {
        "container_id": st.column_config.TextColumn("Container ID"),
        "carrier_name": st.column_config.TextColumn("Carrier"),
        "origin_country": st.column_config.TextColumn("Origin Country"),
        "severity": st.column_config.TextColumn("Severity"),
        "arrive_port_at": st.column_config.DatetimeColumn("Arrive Port At"),
        "depart_port_at": st.column_config.DatetimeColumn("Depart Port At"),
        "port_dwell_days": st.column_config.NumberColumn("Port Dwell (Days)", format="%d"),
        "days_over_sla": st.column_config.NumberColumn("Days Over SLA", format="%d"),
        "estimated_value_at_arrival": st.column_config.NumberColumn(
            "Cargo Value (USD)", format="$%.2f"
        ),
        "demurrage_at_risk_usd": st.column_config.NumberColumn(
            "Demurrage ($)", format="$%.2f"
        ),
        "intermediate_event_count": st.column_config.NumberColumn(
            "Intermediate Events", format="%d"
        ),
    }

    st.dataframe(
        df[display_columns],
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
    )

    csv = df[display_columns].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Export At-Risk List as CSV",
        data=csv,
        file_name=f"port_dwell_bottlenecks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )


def main() -> None:
    st.set_page_config(
        page_title="Supply Chain Operations Dashboard",
        page_icon="🚢",
        layout="wide",
    )

    st.title("🚢 Supply Chain Operations Dashboard")
    st.markdown(
        "Monitor port dwell bottlenecks from the **Gold layer** and act on shipments "
        "exceeding the **5-day SLA**. Powered by Snowflake · dbt · Medallion Architecture."
    )

    daily_demurrage_rate = float(
        _get_config_value("DAILY_DEMURRAGE_RATE", str(DEFAULT_DAILY_DEMURRAGE_RATE))
    )

    try:
        raw_df = load_bottleneck_data()
    except ValueError as exc:
        st.error(str(exc))
        st.info(
            "Configure credentials in `.streamlit/secrets.toml` using keys: "
            "`account`, `user`, `password`, `role`, `warehouse`."
        )
        st.stop()
    except (snowflake.connector.errors.Error, pd.errors.DatabaseError) as exc:
        st.error(
            "Unable to connect to Snowflake. Please check your credentials and try again."
        )
        st.stop()

    if raw_df.empty:
        st.stop()

    enriched_df = enrich_action_list(raw_df, daily_demurrage_rate)

    with st.sidebar:
        st.header("Filters")

        carrier_options = sorted(enriched_df["carrier_name"].dropna().unique())
        origin_options = sorted(enriched_df["origin_country"].dropna().unique())
        severity_options = ["Low", "Medium", "Critical"]

        selected_carriers = st.multiselect(
            "Carrier Name",
            options=carrier_options,
            default=carrier_options,
        )
        selected_origins = st.multiselect(
            "Origin Country",
            options=origin_options,
            default=origin_options,
        )
        selected_severities = st.multiselect(
            "Severity",
            options=severity_options,
            default=severity_options,
        )

        st.divider()
        st.caption(f"Demurrage rate: {format_currency(daily_demurrage_rate)} / day over SLA")
        st.divider()
        st.caption("⚡ Platform Architecture")
        st.caption("• Data Warehouse: Snowflake (Bronze/Silver/Gold)")
        st.caption("• Transformation Layer: dbt Core")
        st.caption("• Pattern Matching: MATCH_RECOGNIZE")
        st.caption("• Dashboard: Streamlit Native Cache")

    filtered_df = enriched_df[
        enriched_df["carrier_name"].isin(selected_carriers)
        & enriched_df["origin_country"].isin(selected_origins)
        & enriched_df["severity"].isin(selected_severities)
    ]

    render_metric_cards(filtered_df)
    st.divider()

    col_left, col_right = st.columns(2)
    with col_left:
        render_carrier_chart(filtered_df)
    with col_right:
        render_origin_chart(filtered_df)

    st.divider()
    render_dwell_distribution(filtered_df)
    st.divider()
    render_action_list(filtered_df)

    st.markdown(
        f"<div style='text-align:right; color: gray; font-size: 12px;'>"
        f"Last refreshed: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
