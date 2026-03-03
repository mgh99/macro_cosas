# connectors/united_nations_wpp_csv.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

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


def _compute_year_window(time_cfg: dict) -> Tuple[int, int]:
    mode = (time_cfg.get("mode") or "").lower()
    cy = current_year()

    if mode == "past_10_years_and_future_10_years":
        return cy - 10, cy + 10

    if mode == "past_years":
        years = int(time_cfg.get("years", 10))
        return cy - years, cy

    if mode == "past_and_future_years":
        past = int(time_cfg.get("past_years", 10))
        future = int(time_cfg.get("future_years", 10))
        return cy - past, cy + future

    return cy - 10, cy + 10


def fetch_indicator(
    csv_gz_url: str,
    csv_cache_path: str,
    geo_iso2: str,
    indicator_code: str,          # e.g. "TPopulation1Jan"
    indicator_name: str,          # pipeline key (you can keep same)
    time_cfg: dict,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
    variant_equals: str = "Medium",
    debug: bool = False,
) -> pd.DataFrame:
    """
    WPP2024 Demographic_Indicators_Medium.csv.gz is WIDE:
      columns include 'Time', 'ISO3_code', 'Variant', and many indicator columns (TPopulation1Jan, ...).
    """
    path = _download_if_missing(csv_gz_url, Path(csv_cache_path))
    df = pd.read_csv(path, compression="gzip", low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]

    required_base = {"ISO3_code", "Time"}
    missing_base = [c for c in required_base if c not in df.columns]
    if missing_base:
        raise ValueError(f"WPP CSV missing base columns {missing_base}. Found: {list(df.columns)}")

    if indicator_code not in df.columns:
        # helpful error showing close candidates
        candidates = [c for c in df.columns if c.lower() == indicator_code.lower()]
        raise ValueError(
            f"WPP CSV does not contain indicator column '{indicator_code}'. "
            f"Found case-insensitive matches: {candidates}. "
            f"Available columns sample: {list(df.columns)[:30]}"
        )

    # 1) country filter
    iso3 = _iso2_to_iso3(geo_iso2)
    df["ISO3_code"] = df["ISO3_code"].astype(str).str.strip()
    df = df[df["ISO3_code"] == iso3].copy()

    # 2) variant filter (if Variant exists)
    if "Variant" in df.columns and variant_equals:
        df["Variant"] = df["Variant"].astype(str).str.strip()
        df = df[df["Variant"] == variant_equals].copy()

    # 3) year filter
    df["Time"] = pd.to_numeric(df["Time"], errors="coerce")
    df = df[df["Time"].notna()].copy()
    df["year"] = df["Time"].astype(int)

    start_y, end_y = _compute_year_window(time_cfg)
    df = df[(df["year"] >= start_y) & (df["year"] <= end_y)].copy()

    # 4) values
    df[indicator_code] = pd.to_numeric(df[indicator_code], errors="coerce")
    df = df[df[indicator_code].notna()].copy()

    if debug:
        print("WPP DEBUG cols:", df.columns.tolist()[:40])
        print("rows:", len(df))
        print(df[["ISO3_code", "Variant"] if "Variant" in df.columns else ["ISO3_code"]].head(3))
        print(df[["year", indicator_code]].head(10))

    sub_short = df["Variant"].astype(str) if "Variant" in df.columns else pd.Series(["TOTAL"] * len(df), index=df.index)

    out = pd.DataFrame(
        {
            "geo": geo_iso2.upper(),
            "geo_level": geo_level,
            "indicator": indicator_name,
            "date": df["year"].astype(int),
            "month": pd.NA,
            "value": df[indicator_code],
            "unit": unit_fallback,
            "source": "united_nations_wpp_csv",
            "sub_indicator_short": sub_short,
        }
    ).dropna(subset=["value"])

    return out.reset_index(drop=True)