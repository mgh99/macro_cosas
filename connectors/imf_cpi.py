# connectors/imf_cpi.py
#
# Conector para el dataset Consumer Price Index (CPI) del IMF Statistics Dept.
# API: https://api.imf.org/external/sdmx/2.1/data/IMF.STA,CPI,5.0.0/
#
# Formato de la key (5 dimensiones):
#   {COUNTRY}.{INDEX_TYPE}.{COICOP_1999}.{TYPE_OF_TRANSFORMATION}.{FREQUENCY}
#
# Valores HICP confirmados:
#   INDEX_TYPE       : HICP
#   COICOP_1999      : _T  (all items), CP01-CP12 (categorías)
#   TRANSFORMATION   : YOY_PCH_PA_PT  (year-over-year, period average, %)
#                      POP_PCH_PA_PT  (period-over-period, period average, %)
#                      IX             (index 2015=100)
#   FREQUENCY        : A (annual), M (monthly), Q (quarterly)
#   COUNTRY          : ISO3 (ESP, FRA, DEU, ...)
#
# Ejemplo de URL:
#   .../ESP.HICP._T.YOY_PCH_PA_PT.A?startPeriod=2016&endPeriod=2025

from __future__ import annotations

from typing import Optional
from xml.etree import ElementTree as ET

import pandas as pd
import requests

BASE_URL = "https://api.imf.org/external/sdmx/2.1/data/IMF.STA,CPI,5.0.0"

# Namespace del XML de respuesta
_NS = "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=IMF.STA:CPI(5.0.0):ObsLevelDim:TIME_PERIOD"


def fetch_indicator(
    geo_iso3: str,
    index_type: str,
    coicop: str,
    transformation: str,
    frequency: str,
    start_year: int,
    end_year: int,
    indicator_name: str,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
) -> pd.DataFrame:
    key = f"{geo_iso3}.{index_type}.{coicop}.{transformation}.{frequency}"
    url = f"{BASE_URL}/{key}"
    params = {"startPeriod": start_year, "endPeriod": end_year}

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    root = ET.fromstring(r.text)

    # Buscar elementos Series en cualquier namespace
    series_list = [e for e in root.iter() if e.tag.split("}")[-1] == "Series"]

    rows = []
    for s in series_list:
        for obs in s:
            tag = obs.tag.split("}")[-1]
            if tag != "Obs":
                continue
            time_val = obs.attrib.get("TIME_PERIOD")
            obs_val = obs.attrib.get("OBS_VALUE")
            try:
                rows.append({
                    "geo": geo_iso3,
                    "date": int(str(time_val)[:4]),
                    "value": float(obs_val),
                })
            except (TypeError, ValueError):
                continue

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=["geo", "geo_level", "indicator", "date", "month", "value", "unit", "source"]
        )

    df["geo_level"] = geo_level
    df["indicator"] = indicator_name
    df["month"] = pd.NA
    df["unit"] = unit_fallback
    df["source"] = f"imf_cpi:{index_type}.{coicop}.{transformation}"
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"]).copy()
    df["date"] = df["date"].astype(int)

    return df[["geo", "geo_level", "indicator", "date", "month", "value", "unit", "source"]]
