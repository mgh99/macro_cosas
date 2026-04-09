# connectors/worldbank.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.worldbank.org/v2"


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "macro-cosas/1.0"})
    return s


_SESSION = _session()


def wb_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    # (connect timeout, read timeout)
    r = _SESSION.get(url, params=params or {}, timeout=(10, 120))
    if not r.ok:
        # World Bank returns 400 when an indicator has no data for a given country,
        # and 502/503 when their API is temporarily unhealthy.
        # Treat any HTTP error as "no data" rather than crashing the whole job.
        return None
    return r.json()


def fetch_indicator(
    indicator_id: str,          # e.g. "SL.UEM.1524.ZS"
    geo_wb3: str,               # e.g. "ESP"
    start_year: int,
    end_year: int,
    indicator_name: str,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
) -> pd.DataFrame:
    """
    World Bank API:
      /country/{geo}/indicator/{indicator}?format=json&date=YYYY:YYYY&per_page=...
    """
    params = {
        "format": "json",
        "date": f"{start_year}:{end_year}",
        "per_page": 2000,  # más pequeño => responde antes (y no suele hacer falta más)
        "page": 1,
    }

    rows: List[Dict[str, Any]] = []

    while True:
        js = wb_get(f"country/{geo_wb3}/indicator/{indicator_id}", params=params)

        # None means 400 → no data available for this country/indicator
        if js is None:
            break

        # Typical: [metadata, [ {...}, {...} ]]
        if not isinstance(js, list) or len(js) < 2 or not isinstance(js[1], list):
            break

        meta = js[0] if isinstance(js[0], dict) else {}
        data = js[1]

        for item in data:
            if not isinstance(item, dict):
                continue
            y = item.get("date")
            v = item.get("value")
            try:
                y_int = int(str(y))
            except Exception:
                continue
            rows.append({"geo": geo_wb3, "date": y_int, "value": v})

        pages = int(meta.get("pages", 1) or 1)
        page = int(meta.get("page", params["page"]) or params["page"])
        if page >= pages:
            break

        params["page"] = page + 1

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["geo","geo_level","indicator","date","month","value","unit","source"])

    df["geo_level"] = geo_level
    df["indicator"] = indicator_name
    df["month"] = pd.NA
    df["unit"] = unit_fallback
    df["source"] = f"worldbank:{indicator_id}"

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"]).copy()
    df["date"] = df["date"].astype(int)

    return df[["geo","geo_level","indicator","date","month","value","unit","source"]]