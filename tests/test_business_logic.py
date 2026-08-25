from datetime import date

import pytest

from src.business_logic import (
    days_to_release,
    transit_days,
    is_below_reorder,
    is_serious_event,
    safe_rate_pct,
    release_rate_pct,
    sae_rate_pct,
    excursion_rate_pct,
)


def test_days_to_release_normal_case():
    assert days_to_release(date(2026, 1, 1), date(2026, 1, 15)) == 14


def test_days_to_release_not_yet_released_returns_none():
    assert days_to_release(date(2026, 1, 1), None) is None


def test_transit_days_same_day_delivery():
    assert transit_days(date(2026, 1, 1), date(2026, 1, 1)) == 0


def test_transit_days_normal_case():
    assert transit_days(date(2026, 1, 1), date(2026, 1, 6)) == 5


@pytest.mark.parametrize(
    "on_hand,reorder,expected",
    [
        (100, 200, True),   # below
        (200, 200, False),  # exactly at reorder point -- not "below"
        (300, 200, False),  # above
        (0, 1, True),       # zero stock, any reorder point
    ],
)
def test_is_below_reorder(on_hand, reorder, expected):
    assert is_below_reorder(on_hand, reorder) is expected


@pytest.mark.parametrize(
    "severity,expected",
    [("SAE", True), ("Severe", False), ("Mild", False), ("Moderate", False)],
)
def test_is_serious_event(severity, expected):
    assert is_serious_event(severity) is expected


def test_safe_rate_pct_zero_denominator_returns_zero_not_error():
    assert safe_rate_pct(5, 0) == 0.0


def test_safe_rate_pct_rounds_to_requested_decimals():
    assert safe_rate_pct(1, 3, decimals=2) == 33.33


def test_release_rate_pct_all_released():
    assert release_rate_pct(100, 100) == 100.0


def test_release_rate_pct_no_batches_yet():
    assert release_rate_pct(0, 0) == 0.0


def test_sae_rate_pct_typical():
    assert sae_rate_pct(3, 120) == 2.5


def test_excursion_rate_pct_typical():
    assert excursion_rate_pct(4, 1000) == 0.4
