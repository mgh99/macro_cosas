import itertools
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"


def eurostat_get(dataset: str, params: Dict[str, Any], lang: str = "EN", fmt: str = "JSON") -> Dict[str, Any]:
    url = f"{BASE_URL}/{dataset}"
    params = {"format": fmt, "lang": lang, **params}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def _ordered_category_codes(dim_obj: Dict[str, Any]) -> List[str]:
    idx = dim_obj["category"]["index"]
    return [code for code, _ in sorted(idx.items(), key=lambda kv: kv[1])]


def jsonstat_to_dataframe(js: Dict[str, Any]) -> pd.DataFrame:
    dims = js["id"]
    dim_meta = js["dimension"]

    categories = []
    for d in dims:
        categories.append(_ordered_category_codes(dim_meta[d]))

    rows = list(itertools.product(*categories))
    df = pd.DataFrame(rows, columns=dims)

    values = js.get("value", [])
    if isinstance(values, list):
        df["value"] = values
    elif isinstance(values, dict):
        df["value"] = [None] * len(df)
        for k, v in values.items():
            df.loc[int(k), "value"] = v
    else:
        raise TypeError(f"Unexpected value type: {type(values)}")

    return df


def normalize(
    df: pd.DataFrame,
    indicator_name: str,
    dataset: str,
    geo: str,
    geo_level: str,
) -> pd.DataFrame:

    out = pd.DataFrame({
        "geo": df.get("geo", geo),
        "geo_level": geo_level,
        "indicator": indicator_name,
        "date": df["time"].astype(str).str.extract(r"(\d{4})")[0].astype(int),
        "value": pd.to_numeric(df["value"], errors="coerce"),
        "unit": df.get("unit"),
        "source": f"eurostat:{dataset}",
    })

    return out.dropna(subset=["value"])


def fetch_indicator(
    dataset: str,
    indicator_name: str,
    geo: str,
    start_year: int,
    end_year: int,
    freq: str,
    unit: str,
    filters: Dict[str, Any],
    geo_level: str,
    lang: str = "EN",
    fmt: str = "JSON",
) -> pd.DataFrame:

    params = {
        "geo": geo,
        "freq": freq,
        "sinceTimePeriod": start_year,
        "untilTimePeriod": end_year,
        "unit": unit,
        **filters,
    }

    js = eurostat_get(dataset, params, lang, fmt)
    df_raw = jsonstat_to_dataframe(js)

    return normalize(df_raw, indicator_name, dataset, geo, geo_level)