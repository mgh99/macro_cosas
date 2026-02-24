from typing import Any, Dict, List, Optional

import pandas as pd
import requests

# IMF Data Services (JSON REST / SDMX_JSON.svc) — widely used endpoint
BASE_URL = "https://dataservices.imf.org/REST/SDMX_JSON.svc"


def imf_get_compact(dataflow: str, key: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Calls IMF CompactData endpoint.
    Example pattern:
      .../CompactData/IFS/ES.PCPI_IX.M?startPeriod=2015&endPeriod=2023
    """
    url = f"{BASE_URL}/CompactData/{dataflow}/{key}"
    r = requests.get(url, params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()


def _ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def parse_compactdata(js: Dict[str, Any]) -> pd.DataFrame:
    """
    Parses IMF CompactData JSON into a DataFrame with columns:
      time, value, series_key(optional)
    Supports single or multiple series in one response.
    """
    # Typical path: js["CompactData"]["DataSet"]["Series"]
    try:
        series = js["CompactData"]["DataSet"].get("Series")
    except Exception:
        # IMF returns different payload on errors; show something useful
        top_keys = list(js.keys()) if isinstance(js, dict) else str(type(js))
        raise ValueError(f"Unexpected IMF response format. Top-level keys: {top_keys}. Snippet: {str(js)[:400]}")

    series_list = _ensure_list(series)
    records: List[Dict[str, Any]] = []

    for s in series_list:
        obs_list = _ensure_list(s.get("Obs"))
        # Keep some identifying attributes (optional)
        series_attrs = {k: v for k, v in s.items() if k.startswith("@")}
        for obs in obs_list:
            # Obs usually has: "@TIME_PERIOD", "@OBS_VALUE"
            t = obs.get("@TIME_PERIOD")
            v = obs.get("@OBS_VALUE")
            if t is None:
                continue
            records.append({
                "time": t,
                "value": v,
                **series_attrs,
            })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"])


def normalize(
    df: pd.DataFrame,
    indicator_name: str,
    dataflow: str,
    geo: str,
    geo_level: str,
    unit: Optional[str] = None,
) -> pd.DataFrame:
    """
    Standard long schema:
      geo, geo_level, indicator, date, value, unit, source
    """
    out = pd.DataFrame({
        "geo": geo,
        "geo_level": geo_level,
        "indicator": indicator_name,
        "date": df["time"].astype(str).str.extract(r"(\d{4})")[0].astype(int),
        "value": pd.to_numeric(df["value"], errors="coerce"),
        "unit": unit,
        "source": f"imf:{dataflow}",
    })
    return out.dropna(subset=["value"])


def fetch_indicator(
    dataflow: str,
    key: str,
    indicator_name: str,
    geo: str,
    start_year: int,
    end_year: int,
    geo_level: str,
    unit: Optional[str] = None,
) -> pd.DataFrame:
    params = {
        "startPeriod": start_year,
        "endPeriod": end_year,
    }
    js = imf_get_compact(dataflow=dataflow, key=key, params=params)
    df_raw = parse_compactdata(js)
    if df_raw.empty:
        return pd.DataFrame(columns=["geo", "geo_level", "indicator", "date", "value", "unit", "source"])

    df_norm = normalize(df_raw, indicator_name, dataflow, geo, geo_level, unit=unit)
    return df_norm[(df_norm["date"] >= start_year) & (df_norm["date"] <= end_year)]