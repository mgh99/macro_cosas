from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from connectors.eurostat import describe_dataset, fetch_indicator
from core.config_loader import load_config


def current_year() -> int:
    return datetime.utcnow().year


def compute_time_window(time_cfg: Dict[str, Any]) -> tuple[int, int]:
    mode = (time_cfg or {}).get("mode", "past_years")
    if mode == "past_years":
        years = int(time_cfg.get("years", 5))
        end_y = current_year() - 1  # último año “cerrado”
        start_y = end_y - (years - 1)
        return start_y, end_y
    raise ValueError(f"Unsupported time mode: {mode}")


def main() -> None:
    cfg = load_config("config/frameworks.yaml")
    frameworks = cfg.get("frameworks", {})

    geos = ["ES", "FR", "DE"]  # ISO2 country codes (Eurostat geo)

    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)

    # 0) Antes de nada: describimos el dataset para saber el nombre real de la dimensión
    #    En tps00010, esto te dirá si se llama 'age' o 'indic_de' o lo que sea.

    #describe_dataset("tps00010", sample_geo="ES")
    # describe una vez por dataset (solo debug)
    seen = set()
    for ind in frameworks["demographics"]["indicators"]:
        if not ind.get("enabled", True):
            continue
        ds = ind["dataset"]
        if ds not in seen:
            describe_dataset(ds, sample_geo="ES", overrides=ind.get("describe_overrides"))
            seen.add(ds)

    all_parts: List[pd.DataFrame] = []

    demo = frameworks["demographics"]
    for ind in demo["indicators"]:
        if not ind.get("enabled", True):
            continue

        dataset = ind["dataset"]
        freq = ind.get("frequency", "A")
        time_cfg = ind.get("time", {"mode": "past_years", "years": 5})
        start_year, end_year = compute_time_window(time_cfg)

        filters = ind.get("filters", {}) or {}
        multi_filters = ind.get("multi_filters", None)

        for geo in geos:
            df = fetch_indicator(
                dataset=dataset,
                geo=geo,
                start_year=start_year,
                end_year=end_year,
                freq=freq,
                unit_fallback = ind.get("units"),  # "percent"
                geo_level=ind.get("geo_level", "country"),
                indicator_name=ind["name"],
                filters=filters,
                multi_filters=multi_filters,
            )
            #df["framework"] = "demographics"
            #df["geo_iso2"] = geo
            all_parts.append(df)

    if not all_parts:
        print("⚠️ No data fetched.")
        return

    wanted_cols = ["geo", "indicator", "date", "value", "unit", "sub_indicator_short"]
    df_long = pd.concat(all_parts, ignore_index=True)
    df_out = df_long[[c for c in wanted_cols if c in df_long.columns]].copy()

    ind_sel = "population_by_age_group"
    excel_path = out_dir / "demographics_views_by_indicator.xlsx"

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        # hoja raw
        df_out.to_excel(writer, sheet_name="raw_long", index=False)

        for ind_name in sorted(df_out["indicator"].unique()):
            sub = df_out[df_out["indicator"] == ind_name].copy()
            if sub.empty:
                continue

            # Si existe sub_indicator_short (como tps00010), filas = geo + sub_indicator_short
            if "sub_indicator_short" in sub.columns and sub["sub_indicator_short"].notna().any():
                table = (
                    sub.pivot_table(
                        index=["geo", "sub_indicator_short"],
                        columns="date",
                        values="value",
                        aggfunc="first",
                    )
                    .reset_index()
                )
            else:
                # Si no, filas = geo
                table = (
                    sub.pivot_table(
                        index=["geo"],
                        columns="date",
                        values="value",
                        aggfunc="first",
                    )
                    .reset_index()
                )

            # nombre de hoja (máx 31 chars)
            sheet = ind_name[:31]
            table.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ Wrote {excel_path}")

    df_out.to_csv(out_dir / "demographics_macro_long.csv", index=False)
    print("✅ Wrote outputs/demographics_macro_long.csv")


if __name__ == "__main__":
    main()