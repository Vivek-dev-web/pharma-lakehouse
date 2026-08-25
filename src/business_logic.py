"""
Pure-Python business rules extracted out of the Lakeflow gold transforms
(transforms/gold.py) so they're unit-testable without spinning up Spark.
The gold notebook calls the Spark-native equivalent of each of these; keep
both in sync if the rule changes.
"""
from datetime import date
from typing import Optional


def days_to_release(manufacture_date: date, qc_release_date: Optional[date]) -> Optional[int]:
    """Days between manufacture and QC release. None if not yet released."""
    if qc_release_date is None:
        return None
    return (qc_release_date - manufacture_date).days


def transit_days(ship_date: date, received_date: date) -> int:
    return (received_date - ship_date).days


def is_below_reorder(on_hand_qty: int, reorder_point: int) -> bool:
    return on_hand_qty < reorder_point


def is_serious_event(severity: str) -> bool:
    return severity == "SAE"


def safe_rate_pct(numerator: int, denominator: int, decimals: int = 2) -> float:
    """Percentage, returning 0.0 (not NaN/inf) when denominator is 0."""
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, decimals)


def release_rate_pct(batches_released: int, batches_total: int) -> float:
    return safe_rate_pct(batches_released, batches_total, decimals=1)


def sae_rate_pct(sae_total: int, events_total: int) -> float:
    return safe_rate_pct(sae_total, events_total)


def excursion_rate_pct(excursions_total: int, shipments_total: int) -> float:
    return safe_rate_pct(excursions_total, shipments_total)
