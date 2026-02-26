# connectors/oecd.py
from __future__ import annotations

from io import StringIO
from typing import Optional, Sequence

import pandas as pd
import requests

from core.geo_mapper import to_iso2

BASE_URL = "https://sdmx.oecd.org/public/rest/data"

def fetch_indicator(
    dataset_id: str,
    selection_template: str,
    geos_iso3: Sequence[str],
    start_period: str,
    end_period: str,
    indicator_name: str,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
) -> pd.DataFrame:
    """
    dataset_id example:
      "OECD.SDD.STES,DSD_STES@DF_CLI,"
    selection_template example (9 dims):
      "{geo}.M.CCICP........H"
      (9 dims total separated by '.')
      Use empty segments for ALL (i.e. consecutive dots)
    """

    geo_block = "+".join(geos_iso3)

    selection = selection_template.format(geo=geo_block).strip()

    # Ensure it starts with "/"
    if not selection.startswith("/"):
        selection = "/" + selection

    url = f"{BASE_URL}/{dataset_id}{selection}"

    params = {
        "startPeriod": start_period,
        "endPeriod": end_period,
        "dimensionAtObservation": "AllDimensions",
        "format": "csvfilewithlabels",
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    raw = pd.read_csv(StringIO(r.text))
    if raw.empty:
        return pd.DataFrame(columns=["geo","geo_level","indicator","date","month","value","unit","source"])

    time_col = "TIME_PERIOD"
    val_col = "OBS_VALUE"

    # In OECD CSV it is usually REF_AREA (not LOCATION)
    geo_col = "REF_AREA" if "REF_AREA" in raw.columns else ("LOCATION" if "LOCATION" in raw.columns else None)
    if geo_col is None:
        raise ValueError(f"Could not find REF_AREA/LOCATION in OECD CSV columns: {list(raw.columns)[:40]}")

    
    tp = raw[time_col].astype(str)

    year = pd.to_numeric(tp.str.slice(0, 4), errors="coerce")

    # Monthly si tiene "-"
    is_monthly = tp.str.contains("-", regex=False)
    month = pd.to_numeric(tp.str.slice(5, 7), errors="coerce")
    month = month.where(is_monthly, pd.NA)  # annual => NA

    out = pd.DataFrame({
        "geo": raw[geo_col].astype(str),
        "geo_level": geo_level,
        "indicator": indicator_name,
        "date": year,
        "month": month,
        "value": pd.to_numeric(raw[val_col], errors="coerce"),
        "unit": unit_fallback,
        "source": f"oecd:{dataset_id}",
    })

    # 👇 IMPORTANTE: no dropear por month
    out = out.dropna(subset=["geo", "date", "value"]).copy()
    out["date"] = out["date"].astype(int)
    if "month" in out.columns:
        out["month"] = pd.to_numeric(out["month"], errors="coerce")
        
    out["geo"] = out["geo"].apply(to_iso2)

    return out