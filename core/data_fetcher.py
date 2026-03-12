# core/data_fetcher.py
from __future__ import annotations

from typing import List

import pandas as pd

from connectors.eurostat import eurostat_get
from connectors.eurostat import fetch_indicator as fetch_eurostat
from connectors.imf_cpi import fetch_indicator as fetch_imf_cpi
from connectors.imf_datamapper import fetch_indicator as fetch_imf
from connectors.oecd import fetch_indicator as fetch_oecd
from connectors.un_tourism_xlsx import fetch_indicator as fetch_un_tourism_xlsx
from connectors.un_tourism_zip import fetch_indicator as fetch_un_tourism_zip
from connectors.united_nations_wpp_age_excel import \
    fetch_indicator as fetch_united_nations_wpp_age_excel
from connectors.united_nations_wpp_csv import \
    fetch_indicator as fetch_united_nations_wpp_csv
from connectors.united_nations_xlsx import \
    fetch_indicator as fetch_united_nations
from connectors.worldbank import fetch_indicator as fetch_worldbank
from core.country_resolver import AGGREGATE_GEO_CODES
from core.geo_mapper import to_imf_geo, to_iso2, to_oecd_geo, to_wb_geo

# Eurostat uses different codes for aggregates; WEOWORLD/ADVEC have no eurostat equivalent
_EUROSTAT_AGGREGATE_MAP = {"EU": "EU27_2020"}
_EMPTY_DF_COLS = ["geo", "geo_level", "indicator", "date", "month", "value", "unit", "source"]
from core.time_utils import (compute_time_window, compute_years_list,
                             current_year)


def _extract_years_from_eurostat_payload(js: dict) -> list[int]:
    """
    Eurostat JSON-stat: dimension['time']['category']['index'] contiene keys tipo:
      "2015" o "2015-01"
    """
    idx = (
        js.get("dimension", {})
          .get("time", {})
          .get("category", {})
          .get("index", {})
    )
    years: list[int] = []
    if isinstance(idx, dict):
        for k in idx.keys():
            s = str(k)
            try:
                years.append(int(s[:4]))
            except Exception:
                continue
    return years


def _get_last_available_year_eurostat(
    dataset: str,
    geo: str,
    freq: str,
    filters: dict,
) -> int:
    """
    Llama a Eurostat con lastTimePeriod=1 para descubrir el último año real disponible
    (para ese geo + filtros). Si falla por alguna dim, hace fallback con params más simples.
    """
    filters = filters or {}

    probe_candidates = [
        {"geo": geo, "freq": freq, "lastTimePeriod": 1, **filters},  # intento “completo”
        {"geo": geo, "lastTimePeriod": 1, **filters},                # sin freq
        {"geo": geo, "freq": freq, "lastTimePeriod": 1},             # sin filters
        {"geo": geo, "lastTimePeriod": 1},                           # mínimo
    ]

    last_err: Exception | None = None
    for params in probe_candidates:
        try:
            js = eurostat_get(dataset, params=params, lang="EN")
            years = _extract_years_from_eurostat_payload(js)
            if years:
                return max(years)
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    raise ValueError(f"Cannot detect last available year for dataset={dataset}, geo={geo}")

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
        # Aggregate geos without a Eurostat equivalent → return empty
        if geo in AGGREGATE_GEO_CODES and geo not in _EUROSTAT_AGGREGATE_MAP:
            return pd.DataFrame(columns=_EMPTY_DF_COLS)
        # Map aggregate codes to Eurostat's own codes (EU → EU27_2020)
        geo = _EUROSTAT_AGGREGATE_MAP.get(geo, geo)

        freq = ind.get("frequency", "A")
        filters = ind.get("filters", {}) or {}
        unit_fallback = ind.get("units")

        # Si ya viene unit en filters, NO uses unit_fallback
        if "unit" in filters:
            unit_fallback = None

        mode = (time_cfg.get("mode") or "").lower()

        if mode == "last_available_years":
            years_n = int(time_cfg.get("years", 10))
            last_y = _get_last_available_year_eurostat(
                dataset=ind["dataset"],
                geo=geo,
                freq=freq,
                filters=filters,
            )
            start_year = last_y - years_n + 1
            end_year = last_y
        else:
            start_year, end_year = compute_time_window(time_cfg)

        return fetch_eurostat(
            dataset=ind["dataset"],
            geo=geo,
            start_year=start_year,
            end_year=end_year,
            freq=freq,
            unit_fallback=ind.get("units"),
            geo_level=ind.get("geo_level", "country"),
            indicator_name=ind["name"],
            filters=filters,
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
            indicator_code_equals=ind.get("indicator_code_equals"),
            indicator_label_equals=ind.get("indicator_label_equals"),
            reporter_area_code_field=ind.get("reporter_area_code_field", "reporter_area_code"),
            partner_area_label_field=ind.get("partner_area_label_field", "partner_area_label"),
            sub_indicator_field=ind.get("sub_indicator_field"),
            partner_area_labels=ind.get("partner_area_labels"),
            debug=ind.get("debug", False), # PARA DEBUG, no afecta la lógica principal
        )
    
    # ==========================
    # UNITED NATIONS (XLSX -> parse)
    # ==========================
    if source in {"united_nations", "united_nations_xlsx", "united-nations-xlsx", "undesa"}:
        
        if ind.get("excel_url"):
            return fetch_united_nations_wpp_age_excel(
                excel_url=ind["excel_url"],
                excel_cache_path=ind.get("excel_cache_path"),
                geo_iso2=geo,
                indicator_name=ind["name"],
                sheet=ind.get("sheet", "Estimates"),
                time_cfg=time_cfg,
                geo_level=ind.get("geo_level", "country"),
                unit_fallback=ind.get("units"),
                debug=ind.get("debug", False),
            )
        
        # UNITED NATIONS (WPP CSV.GZ)
        if ind.get("csv_gz_url"):
            return fetch_united_nations_wpp_csv(
                csv_gz_url=ind["csv_gz_url"],
                csv_cache_path=ind.get("csv_cache_path") or f"data/united_nations_wpp/{ind['name']}.csv.gz",
                geo_iso2=geo,
                indicator_code=ind.get("indicator_code") or ind.get("name"),
                indicator_name=ind["name"],
                time_cfg=time_cfg,
                geo_level=ind.get("geo_level", "country"),
                unit_fallback=ind.get("units"),
                debug=ind.get("debug", False),
            )

        # UNITED NATIONS (XLSX)
        return fetch_united_nations(
            xlsx_url=ind["xlsx_url"],
            xlsx_cache_path=ind.get("xlsx_cache_path") or f"data/united_nations_xlsx/{ind['name']}.xlsx",
            geo_iso2=geo,
            indicator_name=ind["name"],
            sheet=ind.get("sheet", "HH size and composition 2022"),
            allowed_data_source_categories=ind.get("allowed_data_source_categories"),
            geo_level=ind.get("geo_level", "country"),
            unit_fallback=ind.get("units"),
            debug=ind.get("debug", False),
        )

    # ==========================
    # IMF CPI (SDMX XML — api.imf.org)
    # ==========================
    if source in {"imf_cpi", "imf-cpi"}:
        start_year, end_year = compute_time_window(time_cfg)

        df = fetch_imf_cpi(
            geo_iso3=to_imf_geo(geo),   # ISO2 → ISO3
            index_type=ind.get("index_type", "HICP"),
            coicop=ind.get("coicop", "_T"),
            transformation=ind.get("transformation", "YOY_PCH_PA_PT"),
            frequency=ind.get("frequency", "A"),
            start_year=start_year,
            end_year=end_year,
            indicator_name=ind["name"],
            geo_level=ind.get("geo_level", "country"),
            unit_fallback=ind.get("units"),
        )
        df["geo"] = df["geo"].apply(to_iso2)
        return df

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


