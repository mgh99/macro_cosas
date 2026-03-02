# connectors/united_nations_xlsx.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from core.geo_to_m49 import iso2_to_m49


def _download_if_missing(url: str, dst: Path) -> Path:
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and dst.stat().st_size > 0:
        return dst

    headers = {"User-Agent": "Mozilla/5.0 (compatible; macro_cosas/1.0)"}
    with requests.get(url, stream=True, timeout=120, headers=headers) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
    return dst


def _find_col(cols: list[str], startswith: str) -> str:
    """Encuentra una columna por prefijo (case-insensitive)."""
    low = {c.lower(): c for c in cols}
    for c in cols:
        if c.lower().startswith(startswith.lower()):
            return c
    raise ValueError(f"Column starting with '{startswith}' not found. Columns: {cols}")


def fetch_indicator(
    xlsx_url: str,
    xlsx_cache_path: str,
    geo_iso2: str,
    indicator_name: str,
    sheet: str = "HH size and composition 2022",
    allowed_data_source_categories: Optional[list[str]] = None,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
    debug: bool = False,
) -> pd.DataFrame:
    xlsx_path = _download_if_missing(xlsx_url, Path(xlsx_cache_path))

    # En este XLSX el header real empieza en la fila 5 (header=4)
    df = pd.read_excel(xlsx_path, sheet_name=sheet, header=4)
    df.columns = [str(c).strip() for c in df.columns]

    col_iso = "ISO Code"
    col_cat = "Data source category"
    col_year = _find_col(list(df.columns), "Reference date")
    col_val = "Average household size (number of members)"

    required = {col_iso, col_cat, col_year, col_val}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing}. Found: {list(df.columns)}")

    # 1) filtra país (ISO Code aquí es M49 numérico)
    m49 = int(iso2_to_m49(geo_iso2))
    df[col_iso] = pd.to_numeric(df[col_iso], errors="coerce")
    df = df[df[col_iso] == m49].copy()

    # 2) filtra categorías (DHS, DYB, IPOMS, LFS, MICS)
    if allowed_data_source_categories:
        allowed = {x.strip() for x in allowed_data_source_categories}
        df[col_cat] = df[col_cat].astype(str).str.strip()
        df = df[df[col_cat].isin(allowed)].copy()

    # 3) parse year (mezcla de datetime + strings dd/mm/yyyy)
    dt = pd.to_datetime(df[col_year], errors="coerce", dayfirst=True)
    df = df[dt.notna()].copy()
    df["year"] = dt[dt.notna()].dt.year.astype(int)

    # 4) value numeric
    df[col_val] = pd.to_numeric(df[col_val], errors="coerce")
    df = df[df[col_val].notna()].copy()

    # 5) last_available_year: deja el último año por categoría
    df = df.sort_values(["year"])
    df = df.groupby(col_cat, as_index=False).tail(1)

    if debug:
        print("cols:", df.columns.tolist())
        print("rows:", len(df))
        print(df[[col_iso, col_cat, "year", col_val]].head(20))

    out = pd.DataFrame(
        {
            "geo": geo_iso2.upper(),
            "geo_level": geo_level,
            "indicator": indicator_name,
            "date": df["year"].astype(int),
            "month": pd.NA,
            "value": df[col_val],
            "unit": unit_fallback,
            "source": "united_nations_xlsx",
            "sub_indicator_short": df[col_cat].astype(str).str.strip(),
        }
    ).dropna(subset=["value"])

    return out.reset_index(drop=True)