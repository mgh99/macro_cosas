# connectors/oecd.py
from typing import Any, Dict

import pandas as pd
import requests

BASE_URL = "https://stats.oecd.org/sdmx-json/data"


def oecd_get(dataset: str, query: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/{dataset}/{query}/all"
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    try:
        js = r.json()
    except Exception:
        raise ValueError(f"OECD response is not JSON. First 200 chars:\n{r.text[:200]}")

    errors = js.get("errors")
    if errors:
        raise ValueError(
            f"OECD returned errors for URL: {url}\n"
            f"errors: {errors}"
        )

    # SDMX-JSON v2 wraps payload under "data".
    if "data" in js and isinstance(js["data"], dict):
        js = js["data"]

    has_datasets = "dataSets" in js
    has_structure = "structure" in js or "structures" in js

    if not has_datasets or not has_structure:
        keys = list(js.keys()) if isinstance(js, dict) else type(js)
        raise ValueError(
            "OECD response is not parseable SDMX-JSON (missing dataSets/structure).\n"
            f"URL: {url}\n"
            f"Keys: {keys}\n"
            f"Snippet: {str(js)[:400]}"
        )

    return js


def parse_oecd_json(js: Dict[str, Any]) -> pd.DataFrame:
    """
    Basic parser for OECD SDMX-JSON.
    Converts time series into DataFrame with columns: time, value.
    """

    data_sets = js["dataSets"]
    if not data_sets:
        return pd.DataFrame(columns=["time", "value"])

    series_map = data_sets[0].get("series", {})

    structure = js.get("structure")
    if structure is None:
        structures = js.get("structures", [])
        if not structures:
            raise ValueError("OECD payload has no structure information.")
        structure = structures[0]

    obs_dims = structure.get("dimensions", {}).get("observation", [])
    if not obs_dims:
        raise ValueError("OECD payload has no observation dimensions.")

    time_values = obs_dims[0].get("values", [])

    records = []
    for _, series_data in series_map.items():
        observations = series_data.get("observations", {})

        for time_index, obs in observations.items():
            idx = int(time_index)
            if idx >= len(time_values):
                continue

            value = obs[0] if obs else None
            time_label = time_values[idx].get("id")
            if time_label is None:
                continue

            records.append({"time": time_label, "value": value})

    return pd.DataFrame(records)


def normalize(
    df: pd.DataFrame,
    indicator_name: str,
    dataset: str,
    geo: str,
    geo_level: str,
) -> pd.DataFrame:
    years = pd.to_numeric(
        df["time"].astype(str).str.extract(r"(\d{4})")[0],
        errors="coerce",
    )

    out = pd.DataFrame(
        {
            "geo": geo,
            "geo_level": geo_level,
            "indicator": indicator_name,
            "date": years,
            "value": pd.to_numeric(df["value"], errors="coerce"),
            "unit": None,
            "source": f"oecd:{dataset}",
        }
    )

    out = out.dropna(subset=["date", "value"]).copy()
    out["date"] = out["date"].astype(int)
    return out


def fetch_indicator(
    dataset: str,
    query: str,
    indicator_name: str,
    geo: str,
    start_year: int,
    end_year: int,
    geo_level: str,
) -> pd.DataFrame:

    js = oecd_get(dataset, query)
    df_raw = parse_oecd_json(js)

    df_norm = normalize(df_raw, indicator_name, dataset, geo, geo_level)

    return df_norm[(df_norm["date"] >= start_year) & (df_norm["date"] <= end_year)]
