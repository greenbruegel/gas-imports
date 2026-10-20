"""Source-data checks shared by LNG ingestion and materialization."""

import math

CAPACITY_MULTIPLE = 2.0


def capacity_check(value, capacity):
    """Do not interpret a missing/zero capacity as a valid upper bound."""
    try:
        value, capacity = float(value), float(capacity)
    except (TypeError, ValueError):
        return {"status": "not_checked", "reason": "missing_or_invalid_capacity_or_value"}
    if not math.isfinite(value) or not math.isfinite(capacity) or capacity <= 0:
        return {"status": "not_checked", "reason": "missing_or_invalid_capacity_or_value"}
    return {
        "status": "invalid" if value > CAPACITY_MULTIPLE * capacity else "valid",
        "check": "sendout_capacity",
        "thresholdMultiple": CAPACITY_MULTIPLE,
        "capacityGwhPerDay": capacity,
        "reportedGwh": value,
        "capacityMultiple": value / capacity,
    }
