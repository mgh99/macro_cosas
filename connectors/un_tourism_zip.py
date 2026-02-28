# connectors/un_tourism_zip.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from core.cache_manager import ensure_zip_extracted
from core.geo_to_m49 import iso2_to_m49


def _find_header_row(df_preview: pd.DataFrame) -> int | None:
    """
    Busca una fila que contenga “GeoAreaCode” y “TimePeriod” (o “Year”).
    Muchos XLSX vienen con varias filas de título antes del header real.
    """
    for i in range(min(len(df_preview), 40)):
        row = df_preview.iloc[i].astype(str).str.strip().str.lower().tolist()
        has_geo = any("geoareacode" in c for c in row)
        has_time = any(("timeperiod" in c) or (c == "year") or ("year" in c) for c in row)
        if has_geo and has_time:
            return i
    return None


def _load_best_sheet(xlsx_path: Path, preferred_sheet: Optional[str] = None) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)

    candidates = []
    if preferred_sheet:
        candidates.append(preferred_sheet)
    candidates.extend([s for s in xl.sheet_names if s not in candidates])

    for sheet in candidates:
        preview = pd.read_excel(xlsx_path, sheet_name=sheet, header=None, nrows=50)
        header_row = _find_header_row(preview)
        if header_row is None:
            continue

        df = pd.read_excel(xlsx_path, sheet_name=sheet, header=header_row)
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # fallback: devuelve algo para debug
    df = pd.read_excel(xlsx_path, sheet_name=preferred_sheet or 0, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def fetch_indicator(
    zip_path: Optional[str] = None,
    zip_url: Optional[str] = None,
    geo_iso2: str = "",
    indicator_name: str = "",
    series_code: Optional[str] = None,
    xlsx_glob: str = "*.xlsx",
    sheet: Optional[str] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    geo_level: str = "country",
    unit_fallback: Optional[str] = None,
    cache_dir: str = "data/un_tourism_cache",
) -> pd.DataFrame:
    """
    UN Tourism ZIP connector.

    - Soporta ZIP local (zip_path) o remoto (zip_url)
    - Extrae ZIP a cache_dir/indicator_name/
    - Carga la hoja correcta (auto-detect header row)
    - Filtra por SeriesCode (si aplica), GeoAreaCode (M49) y años
    """
    if not zip_path and not zip_url:
        raise ValueError("fetch_indicator requires zip_path (local) or zip_url (remote).")

    safe_folder = indicator_name.strip().replace("/", "_").replace("\\", "_") or "un_tourism"
    extract_dir = ensure_zip_extracted(
        zip_url=zip_url,
        zip_path=Path(zip_path) if zip_path else None,
        target_dir=Path(cache_dir) / safe_folder,
    )

    xlsx_files = sorted(extract_dir.glob(xlsx_glob))
    if not xlsx_files:
        raise FileNotFoundError(f"No XLSX found in extracted zip: {extract_dir} (glob={xlsx_glob})")

    xlsx_path = xlsx_files[0]

    # ✅ carga robusta de la hoja + header
    df = _load_best_sheet(xlsx_path, preferred_sheet=sheet)

    # Normaliza mapeo case-insensitive
    cols_lower = {str(c).strip().lower(): c for c in df.columns}

    def col(name: str) -> str | None:
        return cols_lower.get(name.strip().lower())

    # 1) Series filter (si existe la columna)
    if series_code:
        c_series = col("SeriesCode")
        if c_series:
            df = df[df[c_series].astype(str).str.strip() == str(series_code).strip()]

    # 2) Geo filter (M49)
    c_geo = col("GeoAreaCode")
    if c_geo:
        area = int(iso2_to_m49(geo_iso2.upper()))
        df[c_geo] = pd.to_numeric(df[c_geo], errors="coerce")
        df = df[df[c_geo].notna() & (df[c_geo].astype(int) == area)]
    else:
        # Si no hay GeoAreaCode, intentamos GeoAreaName / CountryName como fallback (opcional)
        c_name = col("GeoAreaName") or col("Country") or col("CountryName")
        if c_name:
            # Esto es un fallback débil (depende del idioma/alias). Mejor M49.
            df = df[df[c_name].astype(str).str.upper().str.contains(geo_iso2.upper(), na=False)]

    # 3) Time column
    time_col = col("TimePeriod") or col("Year")
    if not time_col:
        raise ValueError(
            f"Cannot find TimePeriod/Year in XLSX. Columns: {list(df.columns)}. "
            f"Tip: revisa el sheet='{sheet}' o cambia xlsx_glob."
        )

    df[time_col] = pd.to_numeric(df[time_col], errors="coerce")
    df = df[df[time_col].notna()].copy()
    df[time_col] = df[time_col].astype(int)

    if start_year is not None:
        df = df[df[time_col] >= int(start_year)]
    if end_year is not None:
        df = df[df[time_col] <= int(end_year)]

    # 4) Value column
    value_col = col("Value") or col("OBS_VALUE")
    if not value_col:
        raise ValueError(f"Cannot find Value/OBS_VALUE in XLSX. Columns: {list(df.columns)}")

    out = pd.DataFrame(
        {
            "geo": geo_iso2.upper(),
            "geo_level": geo_level,
            "indicator": indicator_name,
            "date": df[time_col].astype(int),
            "month": pd.NA,
            "value": pd.to_numeric(df[value_col], errors="coerce"),
            "unit": unit_fallback,
            "source": "un_tourism_zip",
        }
    ).dropna(subset=["value"])

    return out.reset_index(drop=True)
