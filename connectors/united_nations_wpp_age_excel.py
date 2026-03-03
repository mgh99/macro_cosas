# connectors/united_nations_wpp_age_excel.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pycountry
import requests

from core.time_utils import current_year


def _download_if_missing(url: str, dst: Path) -> Path:
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and dst.stat().st_size > 0:
        return dst

    headers = {"User-Agent": "Mozilla/5.0 (compatible; macro_cosas/1.0)"}
    with requests.get(url, stream=True, timeout=180, headers=headers) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
    return dst


def _iso2_to_iso3(iso2: str) -> str:
    c = pycountry.countries.get(alpha_2=iso2.upper())
    if not c:
        raise ValueError(f"Invalid ISO2: {iso2}")
    return c.alpha_3


def _pick_country_col(cols: List[str]) -> Tuple[str, str]:
    """
    Return (column_name, key_type) where key_type in {"iso3","iso2"}.
    Tries common WPP naming variants.
    """
    norm = {c.strip().lower(): c for c in cols}

    # Most common in WPP CSV, sometimes also in XLSX
    for k in ["iso3_code", "iso3 code", "iso3 alpha-code", "iso3 alpha code", "iso3"]:
        if k in norm:
            return norm[k], "iso3"

    for k in ["iso2_code", "iso2 code", "iso2"]:
        if k in norm:
            return norm[k], "iso2"

    raise ValueError(
        "No ISO country column found. Expected one of: ISO3_code / ISO2_code (or similar). "
        f"Columns: {cols[:40]}"
    )


def _build_five_year_groups() -> List[Tuple[str, List[str]]]:
    """
    From single-age columns '0'..'99' and '100+' build groups:
    0-4, 5-9, ... 95-99, 100+
    Returns list of (group_label, age_columns_list)
    """
    groups: List[Tuple[str, List[str]]] = []
    for start in range(0, 100, 5):
        end = start + 4
        label = f"{start}-{end}"
        cols = [str(a) for a in range(start, end + 1)]
        groups.append((label, cols))
    groups.append(("100+", ["100+"]))
    return groups


def _detect_header_row(excel_path: Path, sheet: str) -> int:
    """
    Scan top rows and detect the row that contains actual headers
    (e.g., 'ISO3 Alpha-code' and 'Year/Time').
    """
    probe = pd.read_excel(excel_path, sheet_name=sheet, header=None, nrows=40)
    for idx, row in probe.iterrows():
        vals = {str(v).strip().lower() for v in row.tolist() if pd.notna(v)}
        if not vals:
            continue
        has_iso = ("iso3 alpha-code" in vals) or ("iso3_code" in vals) or ("iso3 code" in vals)
        has_time = ("year" in vals) or ("time" in vals)
        if has_iso and has_time:
            return int(idx)
    return 0


def _normalize_col_name(col: object) -> str:
    """
    Normalize Excel header values:
    - numeric headers 0.0..99.0 -> '0'..'99'
    - text headers kept trimmed
    """
    if isinstance(col, float) and col.is_integer():
        return str(int(col))
    if isinstance(col, int):
        return str(col)
    return str(col).strip()


def fetch_indicator(
    excel_url: str,
    excel_cache_path: str,
    geo_iso2: str,
    indicator_name: str,
    sheet: str,
    time_cfg: dict,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
    debug: bool = False,
) -> pd.DataFrame:
    """
    WPP POP SINGLE AGE file: columns typically include ages '0'..'100+'.
    We aggregate into five-year groups + 100+.
    """
    path = _download_if_missing(excel_url, Path(excel_cache_path))
    header_row = _detect_header_row(path, sheet)
    df = pd.read_excel(path, sheet_name=sheet, header=header_row)
    df.columns = [_normalize_col_name(c) for c in df.columns]

    # --- country filter (robust) ---
    country_col, key_type = _pick_country_col(df.columns)
    iso3 = _iso2_to_iso3(geo_iso2)

    df[country_col] = df[country_col].astype(str).str.strip()
    if key_type == "iso3":
        df = df[df[country_col].str.upper() == iso3].copy()
    else:
        df = df[df[country_col].str.upper() == geo_iso2.upper()].copy()

    # --- year filter ---
    time_col = "Time" if "Time" in df.columns else ("Year" if "Year" in df.columns else None)
    if time_col is None:
        raise ValueError(f"Missing 'Time/Year' column. Columns: {df.columns.tolist()[:40]}")

    df[time_col] = pd.to_numeric(df[time_col], errors="coerce")
    df = df[df[time_col].notna()].copy()
    df["year"] = df[time_col].astype(int)

    cy = current_year()
    years = int(time_cfg.get("years", 5))
    start_y, end_y = cy - years, cy
    df = df[(df["year"] >= start_y) & (df["year"] <= end_y)].copy()

    if df.empty:
        # return empty but correctly shaped DF
        return pd.DataFrame(columns=["geo", "indicator", "date", "month", "value", "unit", "sub_indicator_short"])

    # --- detect age columns present ---
    five_year_groups = _build_five_year_groups()

    # We only keep columns that exist
    existing_age_cols = set(df.columns)

    # compute group sums
    rows_out = []
    for label, cols in five_year_groups:
        cols_present = [c for c in cols if c in existing_age_cols]
        if not cols_present:
            continue

        vals = df[cols_present].apply(pd.to_numeric, errors="coerce")
        group_sum = vals.sum(axis=1, skipna=True)

        tmp = pd.DataFrame(
            {
                "geo": geo_iso2.upper(),
                "geo_level": geo_level,
                "indicator": indicator_name,
                "date": df["year"].astype(int),
                "month": pd.NA,
                "value": group_sum,
                "unit": unit_fallback,
                "source": "united_nations_wpp_age_excel",
                "sub_indicator_short": label,
            }
        )
        tmp = tmp.dropna(subset=["value"])
        rows_out.append(tmp)

    out = pd.concat(rows_out, ignore_index=True) if rows_out else pd.DataFrame()

    if debug:
        print("AGE FILE DEBUG country_col:", country_col, "key_type:", key_type)
        print("Rows after filters:", len(df))
        print("Output rows:", len(out))
        print("Output head:\n", out.head(10))

    return out.reset_index(drop=True)
