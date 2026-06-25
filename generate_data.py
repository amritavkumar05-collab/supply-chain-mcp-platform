#!/usr/bin/env python3
"""Synthesize a supply chain logistics event stream dataset."""

from __future__ import annotations

import random
from datetime import timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

NUM_CONTAINERS = 10_000
DELAY_FRACTION = 0.12
EVENT_SEQUENCE = [
    "DEPART_ORIGIN",
    "ARRIVE_PORT",
    "PORT_DWELL",
    "DEPART_PORT",
    "ARRIVE_DC",
]
OUTPUT_PATH = Path("data/raw_movement_events.parquet")

CARRIERS = [
    "Maersk Line",
    "MSC Mediterranean",
    "CMA CGM",
    "COSCO Shipping",
    "Hapag-Lloyd",
    "ONE Ocean Network",
    "Evergreen Marine",
    "Yang Ming",
    "HMM Co.",
    "ZIM Integrated",
]

ORIGIN_COUNTRIES = [
    "China",
    "Vietnam",
    "India",
    "Germany",
    "South Korea",
    "Japan",
    "Mexico",
    "Brazil",
    "Thailand",
    "Indonesia",
]


def _next_event_time(current, event_type: str, delayed: bool, dwell_index: int = 0) -> pd.Timestamp:
    """Advance event_time according to event_type and delay profile."""
    if event_type == "DEPART_ORIGIN":
        return current
    if event_type == "ARRIVE_PORT":
        return current + timedelta(days=random.randint(5, 30), hours=random.randint(0, 23))
    if event_type == "PORT_DWELL":
        if delayed and dwell_index > 0:
            return current + timedelta(hours=random.randint(6, 36))
        return current + timedelta(hours=random.randint(4, 48))
    if event_type == "DEPART_PORT":
        if delayed:
            return current + timedelta(days=random.randint(6, 14), hours=random.randint(0, 12))
        return current + timedelta(hours=random.randint(6, 72))
    if event_type == "ARRIVE_DC":
        return current + timedelta(days=random.randint(1, 7), hours=random.randint(0, 23))
    raise ValueError(f"Unknown event_type: {event_type}")


def _build_container_events(
    container_id: str,
    carrier_name: str,
    origin_country: str,
    start_time: pd.Timestamp,
    delayed: bool,
    delay_mode: str | None,
) -> list[dict]:
    """Build chronologically ordered events for one container."""
    events: list[dict] = []
    current_time = start_time
    dwell_count = random.randint(2, 5) if delayed and delay_mode == "multiple_dwell" else 1
    dwell_written = 0

    for event_type in EVENT_SEQUENCE:
        if event_type == "PORT_DWELL":
            while dwell_written < dwell_count:
                dwell_written += 1
                current_time = _next_event_time(
                    current_time,
                    event_type,
                    delayed=delayed,
                    dwell_index=dwell_written - 1,
                )
                events.append(
                    {
                        "container_id": container_id,
                        "carrier_name": carrier_name,
                        "event_time": current_time,
                        "event_type": event_type,
                        "estimated_value": round(random.uniform(5_000, 250_000), 2),
                        "origin_country": origin_country,
                    }
                )
            continue

        if event_type == "DEPART_PORT" and delayed and delay_mode == "time_gap":
            current_time = _next_event_time(current_time, event_type, delayed=True)
        else:
            current_time = _next_event_time(current_time, event_type, delayed=False)

        events.append(
            {
                "container_id": container_id,
                "carrier_name": carrier_name,
                "event_time": current_time,
                "event_type": event_type,
                "estimated_value": round(random.uniform(5_000, 250_000), 2),
                "origin_country": origin_country,
            }
        )

    return events


def generate_dataset(seed: int = 42) -> pd.DataFrame:
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    delay_count = int(NUM_CONTAINERS * DELAY_FRACTION)
    container_indices = list(range(NUM_CONTAINERS))
    random.shuffle(container_indices)
    delayed_indices = set(container_indices[:delay_count])
    gap_indices = set(container_indices[: delay_count // 2])
    dwell_indices = delayed_indices - gap_indices

    all_events: list[dict] = []
    base_start = pd.Timestamp("2024-01-01")

    for idx in range(NUM_CONTAINERS):
        container_id = f"CNT-{idx + 1:06d}"
        carrier_name = fake.random_element(CARRIERS)
        origin_country = fake.random_element(ORIGIN_COUNTRIES)
        start_time = base_start + timedelta(
            days=random.randint(0, 300),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        delayed = idx in delayed_indices
        delay_mode = None
        if idx in gap_indices:
            delay_mode = "time_gap"
        elif idx in dwell_indices:
            delay_mode = "multiple_dwell"

        all_events.extend(
            _build_container_events(
                container_id=container_id,
                carrier_name=carrier_name,
                origin_country=origin_country,
                start_time=start_time,
                delayed=delayed,
                delay_mode=delay_mode,
            )
        )

    df = pd.DataFrame(all_events)
    df = df.sort_values(["container_id", "event_time"]).reset_index(drop=True)
    return df


def main() -> None:
    df = generate_dataset()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")

    delay_containers = int(NUM_CONTAINERS * DELAY_FRACTION)
    print(f"Generated {len(df):,} events for {NUM_CONTAINERS:,} containers.")
    print(f"Injected delay patterns for {delay_containers:,} containers ({DELAY_FRACTION:.0%}).")
    print(f"Saved to {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
