# connectors/un_tourism_xlsx.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests

from core.geo_to_m49 import iso2_to_m49


def _download_if_missing(url: str, dst: Path) -> Path:
    dst = Path(dst)
    meta = dst.with_suffix(dst.suffix + ".url")
    dst.parent.mkdir(parents=True, exist_ok=True)

    if dst.exists() and dst.stat().st_size > 0 and meta.exists():
        if meta.read_text(encoding="utf-8").strip() == url.strip():
            return dst  # cache OK

    # (re)download
    headers = {"User-Agent": "Mozilla/5.0 (compatible; macro_cosas/1.0)"}
    with requests.get(url, stream=True, timeout=120, headers=headers) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    meta.write_text(url.strip(), encoding="utf-8")
    return dst


def _pick_sheet(xlsx_path: Path, preferred: Optional[str]) -> str:
    xl = pd.ExcelFile(xlsx_path)
    if preferred and preferred in xl.sheet_names:
        return preferred
    if "Data" in xl.sheet_names:
        return "Data"
    # fallback: busca una hoja que tenga indicator_code / year
    for s in xl.sheet_names:
        df0 = pd.read_excel(xlsx_path, sheet_name=s, nrows=5)
        cols = {str(c).strip().lower() for c in df0.columns}
        if "indicator_code" in cols or "year" in cols:
            return s
    return xl.sheet_names[0]


def fetch_indicator(
    xlsx_url: str,
    xlsx_cache_path: str,
    geo_iso2: str,
    indicator_name: str,
    sheet: Optional[str] = "Data",
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
    indicator_code_prefix: Optional[str] = None,
    indicator_code_equals: Optional[Union[str, list[str]]] = None,
    indicator_label_equals: Optional[Union[str, list[str]]] = None,
    reporter_area_code_field: str = "reporter_area_code",
    partner_area_label_field: str = "partner_area_label",
    sub_indicator_field: Optional[str] = None,
    partner_area_labels: Optional[list[str]] = None,
    debug: bool = False,  # ✅ opcional
) -> pd.DataFrame:
    xlsx_path = _download_if_missing(xlsx_url, Path(xlsx_cache_path))

    sheet_to_use = _pick_sheet(xlsx_path, sheet)
    df = pd.read_excel(xlsx_path, sheet_name=sheet_to_use)
    df.columns = [str(c).strip() for c in df.columns]

    # columnas obligatorias
    required = {reporter_area_code_field, "year", "value"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing}. Found: {list(df.columns)}")

    # 1) filtra país (reporter_area_code es M49)
    m49 = int(iso2_to_m49(geo_iso2))
    df[reporter_area_code_field] = pd.to_numeric(df[reporter_area_code_field], errors="coerce")
    df = df[df[reporter_area_code_field] == m49].copy()

    # 2) filtra indicator_code (equals tiene prioridad sobre prefix)
    if indicator_label_equals is not None:
        if "indicator_label" not in df.columns:
            raise ValueError(f"indicator_label not found. Columns: {list(df.columns)}")

        if isinstance(indicator_label_equals, str):
            lab_list = [indicator_label_equals.strip()]
        else:
            lab_list = [str(x).strip() for x in indicator_label_equals if str(x).strip()]

        df["indicator_label"] = df["indicator_label"].astype(str).str.strip()
        df = df[df["indicator_label"].isin(lab_list)].copy()

    if indicator_code_equals is not None:
        if "indicator_code" not in df.columns:
            raise ValueError(f"indicator_code not found. Columns: {list(df.columns)}")

        if isinstance(indicator_code_equals, str):
            eq_list = [indicator_code_equals.strip()]
        else:
            eq_list = [str(x).strip() for x in indicator_code_equals if str(x).strip()]

        df["indicator_code"] = df["indicator_code"].astype(str).str.strip()
        df = df[df["indicator_code"].isin(eq_list)].copy()

    elif indicator_code_prefix:
        if "indicator_code" not in df.columns:
            raise ValueError(f"indicator_code not found. Columns: {list(df.columns)}")
        pref = indicator_code_prefix.strip()
        df["indicator_code"] = df["indicator_code"].astype(str).str.strip()
        df = df[df["indicator_code"].str.startswith(pref)].copy()

    # 3) filtra regiones/labels (solo si aplica)
    if partner_area_labels:
        if partner_area_label_field not in df.columns:
            raise ValueError(f"{partner_area_label_field} not found. Columns: {list(df.columns)}")
        allowed = {x.strip() for x in partner_area_labels}
        df[partner_area_label_field] = df[partner_area_label_field].astype(str).str.strip()
        df = df[df[partner_area_label_field].isin(allowed)].copy()

    # 4) años
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)

    if start_year is not None:
        df = df[df["year"] >= int(start_year)]
    if end_year is not None:
        df = df[df["year"] <= int(end_year)]

    if debug:
        print("cols:", df.columns.tolist())
        print("rows:", len(df))
        print(df.head(3))
        if "indicator_code" in df.columns:
            print("indicator_code sample:", df["indicator_code"].dropna().astype(str).unique()[:20])

    # 5) output
    # Si no pasas sub_indicator_field, se mantiene el comportamiento actual (partner_area_label)
    sub_field = sub_indicator_field or partner_area_label_field

    out = pd.DataFrame(
        {
            "geo": geo_iso2.upper(),
            "geo_level": geo_level,
            "indicator": indicator_name,
            "date": df["year"].astype(int),
            "month": pd.NA,
            "value": pd.to_numeric(df["value"], errors="coerce"),
            "unit": unit_fallback,
            "source": "un_tourism_xlsx",
            "sub_indicator_short": df[sub_field].astype(str).str.strip() if sub_field in df.columns else pd.NA,
        }
    ).dropna(subset=["value"])

    return out.reset_index(drop=True)