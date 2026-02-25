# connectors/imf_datamapper.py
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
import requests

BASE = "https://www.imf.org/external/datamapper/api/v1"

def fetch_indicator(
    indicator_id: str,
    geo_iso3: str,
    start_year: int,
    end_year: int,
    indicator_name: str,
    geo_level: str = "country",
    unit: Optional[str] = None,
) -> pd.DataFrame:
    periods = ",".join(str(y) for y in range(start_year, end_year + 1))
    url = f"{BASE}/{indicator_id}/{geo_iso3}"
    r = requests.get(url, params={"periods": periods}, timeout=60)
    r.raise_for_status()
    js: Dict[str, Any] = r.json()

    # Esperado: {"values": {"NGDP_RPCH": {"ESP": {"2019":..., "2020":...}}}}
    values = js.get("values", {}).get(indicator_id, {}).get(geo_iso3, {})
    if not values:
        return pd.DataFrame(columns=["geo", "geo_level", "indicator", "date", "value", "unit", "source"])

    rows = []
    for year_str, val in values.items():
        try:
            year = int(year_str)
        except ValueError:
            continue
        rows.append({"date": year, "value": val})

    df = pd.DataFrame(rows).sort_values("date")
    df["geo"] = geo_iso3
    df["geo_level"] = geo_level
    df["indicator"] = indicator_name
    df["unit"] = unit
    df["source"] = f"imf_datamapper:{indicator_id}"
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"])