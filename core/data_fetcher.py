# core/data_fetcher.py
from __future__ import annotations

from typing import List

import pandas as pd

from connectors.eurostat import fetch_indicator as fetch_eurostat
from connectors.imf_datamapper import fetch_indicator as fetch_imf
from connectors.oecd import fetch_indicator as fetch_oecd
from connectors.un_tourism_xlsx import fetch_indicator as fetch_un_tourism_xlsx
from connectors.un_tourism_zip import fetch_indicator as fetch_un_tourism_zip
from connectors.worldbank import fetch_indicator as fetch_worldbank
from core.geo_mapper import to_imf_geo, to_iso2, to_oecd_geo, to_wb_geo
from core.time_utils import (compute_time_window, compute_years_list,
                             current_year)


def fetch_indicator_for_geo(ind: dict, geo: str) -> pd.DataFrame:
    """
    Per-geo fetcher (1 country per call). Ideal for Eurostat + IMF + World Bank + UN Tourism ZIP.
    """
    source = (ind.get("source") or "eurostat").lower()
    time_cfg = ind.get("time", {}) or {}

    # ==========================
    # Eurostat
    # ==========================
    if source == "eurostat":
        start_year, end_year = compute_time_window(time_cfg)
        return fetch_eurostat(
            dataset=ind["dataset"],
            geo=geo,
            start_year=start_year,
            end_year=end_year,
            freq=ind.get("frequency", "A"),
            unit_fallback=ind.get("units"),
            geo_level=ind.get("geo_level", "country"),
            indicator_name=ind["name"],
            filters=ind.get("filters", {}) or {},
            multi_filters=ind.get("multi_filters"),
        )

    # ==========================
    # IMF DataMapper
    # ==========================
    if source in {"imf", "imf_datamapper", "imf-datamapper"}:
        years = compute_years_list(time_cfg, current_year())
        indicator_code = ind.get("indicator_code") or ind.get("dataset")
        if not indicator_code:
            raise ValueError("IMF indicator missing indicator_code (or dataset)")

        return fetch_imf(
            indicator_code=indicator_code,
            geo_imf3=to_imf_geo(geo),
            years=years,
            indicator_name=ind["name"],
            geo_level=ind.get("geo_level", "country"),
            unit_fallback=ind.get("units"),
        )

    # ==========================
    # World Bank
    # ==========================
    if source in {"world_bank", "world_bank_group", "worldbank", "wb"}:
        start_year, end_year = compute_time_window(time_cfg)
        indicator_id = ind.get("indicator_id") or ind.get("indicator_code") or ind.get("dataset")
        if not indicator_id:
            raise ValueError("World Bank indicator missing indicator_id (or indicator_code/dataset)")

        df = fetch_worldbank(
            indicator_id=indicator_id,
            geo_wb3=to_wb_geo(geo),
            start_year=start_year,
            end_year=end_year,
            indicator_name=ind["name"],
            geo_level=ind.get("geo_level", "country"),
            unit_fallback=ind.get("units"),
        )

        # normalize geo to ISO2 for the whole pipeline
        df["geo"] = df["geo"].apply(to_iso2)
        return df

    # ==========================
    # UN Tourism (ZIP -> XLSX -> parse)
    # ==========================
    if source in {"un_tourism_zip", "un-tourism-zip"}:
        start_year, end_year = compute_time_window(time_cfg)

        zip_path = ind.get("zip_path")
        zip_url = ind.get("zip_url")

        if not zip_path and not zip_url:
            raise ValueError("un_tourism_zip indicator requires 'zip_path' (local) or 'zip_url' (remote) in YAML")

        return fetch_un_tourism_zip(
            zip_path=zip_path,
            zip_url=zip_url,
            xlsx_glob=ind.get("xlsx_glob", "*.xlsx"),
            sheet=ind.get("sheet"),
            series_code=ind.get("series_code"),
            geo_iso2=geo,
            indicator_name=ind["name"],
            start_year=start_year,
            end_year=end_year,
            geo_level=ind.get("geo_level", "country"),
            unit_fallback=ind.get("units"),
            cache_dir=ind.get("cache_dir", "data/un_tourism_cache"),
        )
    
    # ==========================
    # UN Tourism (XLSX -> parse)
    # ==========================
    if source in {"un_tourism_xlsx", "un-tourism-xlsx"}:
        start_year, end_year = compute_time_window(time_cfg)

        return fetch_un_tourism_xlsx(
            xlsx_url=ind["xlsx_url"],
            xlsx_cache_path=ind.get("xlsx_cache_path") or f"data/un_tourism_xlsx/{ind['name']}.xlsx",
            geo_iso2=geo,
            indicator_name=ind["name"],
            sheet=ind.get("sheet", "Data"),
            start_year=start_year,
            end_year=end_year,
            geo_level=ind.get("geo_level", "country"),
            unit_fallback=ind.get("units"),
            indicator_code_prefix=ind.get("indicator_code_prefix"),
            reporter_area_code_field=ind.get("reporter_area_code_field", "reporter_area_code"),
            partner_area_label_field=ind.get("partner_area_label_field", "partner_area_label"),
            partner_area_labels=ind.get("partner_area_labels"),
        )

    raise ValueError(f"fetch_indicator_for_geo does not support source: {source}")


def fetch_indicator_for_geos(ind: dict, geos: List[str]) -> pd.DataFrame:
    """
    Multi-geo fetcher (many countries per call). Ideal for OECD.
    Returns ONE dataframe with all requested geos.
    """
    source = (ind.get("source") or "").lower()
    if source != "oecd":
        raise ValueError(f"fetch_indicator_for_geos only supports OECD, got: {source}")

    time_cfg = ind.get("time", {}) or {}
    start_year, end_year = compute_time_window(time_cfg)

    if ind.get("frequency", "A") == "M":
        start_period = f"{start_year}-01"
        end_period = f"{end_year}-12"
    else:
        start_period = str(start_year)
        end_period = str(end_year)

    dataset_id = ind.get("dataset_id")
    selection_template = ind.get("selection_template")
    if not dataset_id or not selection_template:
        raise ValueError("OECD indicator requires dataset_id and selection_template in YAML")

    return fetch_oecd(
        dataset_id=dataset_id,
        selection_template=selection_template,
        geos_iso3=[to_oecd_geo(g) for g in geos],
        start_period=start_period,
        end_period=end_period,
        indicator_name=ind["name"],
        geo_level=ind.get("geo_level", "country"),
        unit_fallback=ind.get("units"),
    )