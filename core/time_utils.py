# core/time_utils.py
from __future__ import annotations

from datetime import datetime
from typing import Dict, List


def current_year() -> int:
    return datetime.now().year


def compute_time_window(time_cfg: Dict, current: int | None = None) -> tuple[int, int]:
    """
    Para EUROSTAT (since/untilTimePeriod).
    """
    current = current or current_year()
    mode = (time_cfg or {}).get("mode", "past_years")
    years = int((time_cfg or {}).get("years", 10))

    if mode == "past_years":
        end_y = current - 1
        start_y = end_y - (years - 1)
        return start_y, end_y

    if mode == "future_years":
        start_y = current
        end_y = current + (years - 1)
        return start_y, end_y

    raise ValueError(f"Unsupported time mode for window-based source: {mode}")


def compute_years_list(time_cfg: Dict, current: int | None = None) -> List[int]:
    """
    Para IMF DataMapper (periods=YYYY,YYYY,...).
    """
    current = current or current_year()
    mode = (time_cfg or {}).get("mode", "past_years")

    if mode == "past_years":
        years = int(time_cfg.get("years", 10))
        end_y = current - 1
        start_y = end_y - (years - 1)
        return list(range(start_y, end_y + 1))

    if mode == "future_years":
        years = int(time_cfg.get("years", 10))
        start_y = current
        end_y = current + (years - 1)
        return list(range(start_y, end_y + 1))

    if mode == "past_and_future_years":
        past = int(time_cfg.get("past_years", 5))
        fut = int(time_cfg.get("future_years", 5))
        past_years = list(range(current - past, current))
        future_years = list(range(current, current + fut))
        return past_years + future_years

    raise ValueError(f"Unsupported time mode for list-based source: {mode}")