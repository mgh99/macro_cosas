# connectors/imf_datamapper.py
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
import requests

BASE_URL = "https://www.imf.org/external/datamapper/api/v1"

def imf_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    r = requests.get(url, params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_indicator(
    indicator_code: str,          # e.g. "LP"
    geo_imf3: str,                # e.g. "ESP"
    years: list[int],
    indicator_name: str,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
) -> pd.DataFrame:
    # DataMapper supports ?periods=YYYY,YYYY (comma-separated)
    params = {"periods": ",".join(str(y) for y in years)} if years else {}

    js = imf_get(f"{indicator_code}/{geo_imf3}", params=params)

    # Typical shape (conceptually): { "values": { "LP": { "ESP": { "2020": x, ... } } }, ... }
    values = js.get("values", {}) or {}
    ind_block = values.get(indicator_code, {}) or {}
    series = ind_block.get(geo_imf3, {}) or {}

    rows = []
    for y_str, v in series.items():
        try:
            y = int(y_str)
        except Exception:
            continue
        rows.append({"geo": geo_imf3, "date": y, "value": v})

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["geo", "geo_level", "indicator", "date", "month", "value", "unit", "source"])

    df["geo_level"] = geo_level
    df["indicator"] = indicator_name
    df["month"] = pd.NA
    df["unit"] = unit_fallback
    df["source"] = f"imf_datamapper:{indicator_code}"

    # coercions
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"]).copy()
    df["date"] = df["date"].astype(int)

    return df[["geo", "geo_level", "indicator", "date", "month", "value", "unit", "source"]]