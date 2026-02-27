from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

from ai.demographics_analyzer import generate_demographics_briefing
from ai.economics_analyzer import generate_economics_briefing
from ai.tourism_analyzer import generate_tourism_briefing
from connectors.eurostat import describe_dataset
from core.config_loader import load_config
from core.data_fetcher import fetch_indicator_for_geo, fetch_indicator_for_geos
from core.excel_single_sheet import build_views_single_sheet_workbook
from core.prompt_loader import load_prompts
from core.seasonality import calculate_seasonality
from core.top_origins import top_origins


def main() -> None:
    cfg = load_config("config/frameworks.yaml")
    frameworks = cfg.get("frameworks", {})

    geos = ["ES", "FR", "DE"]

    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)

    # ==========================
    # Debug: describe EUROSTAT datasets once
    # ==========================
    seen = set()
    for fw in frameworks.values():
        for ind in fw.get("indicators", []):
            if not ind.get("enabled", True):
                continue

            source = (ind.get("source") or "eurostat").lower()
            if source != "eurostat":
                continue

            ds = ind.get("dataset")
            if not ds:
                continue

            if ds not in seen:
                describe_dataset(ds, sample_geo="ES", overrides=ind.get("describe_overrides"))
                seen.add(ds)

    # ==========================
    # Run each framework
    # ==========================
    results_by_framework: Dict[str, pd.DataFrame] = {}
    seasonality_by_framework: Dict[str, pd.DataFrame] = {}

    for fw_name, fw in frameworks.items():
        all_parts: List[pd.DataFrame] = []

        for ind in fw.get("indicators", []):
            if not ind.get("enabled", True):
                continue

            source = (ind.get("source") or "eurostat").lower()

            if source == "oecd":
                df = fetch_indicator_for_geos(ind, geos)
                all_parts.append(df)
            else:
                for geo in geos:
                    df = fetch_indicator_for_geo(ind, geo)
                    all_parts.append(df)

        if not all_parts:
            print(f"⚠️ No data fetched for framework: {fw_name}")
            continue

        wanted_cols = ["geo", "indicator", "date", "month", "value", "unit", "sub_indicator_short"]
        df_long = pd.concat(all_parts, ignore_index=True)
        df_out = df_long[[c for c in wanted_cols if c in df_long.columns]].copy()


        # ==========================
        # Tourism extras
        # ==========================
        if fw_name == "tourism":
            ind_1b = "arrivals_by_origin_hotels_number"
            df_1b = df_out[df_out["indicator"] == ind_1b].copy()

            if df_1b.empty or "sub_indicator_short" not in df_1b.columns:
                print("⚠️ Top origins skipped: sub_indicator_short missing (did you preserve c_resid?)")
            else:
                exclude = {"TOTAL", "EU27_2020", "EA20", "EA19", "EU28"}
                df_1b = df_1b[~df_1b["sub_indicator_short"].isin(exclude)]

                top10 = top_origins(df_1b, top_n=10)
                outp = out_dir / "tourism_arrivals_top10_origins.csv"
                top10.to_csv(outp, index=False)
                print(f"✅ Wrote {outp}")

            monthly_indicator_name = "nights_spent_monthly_hotels"
            monthly = df_out[df_out["indicator"] == monthly_indicator_name].copy()

            if monthly.empty:
                print("⚠️ Seasonality skipped (monthly indicator not found).")
            else:
                seasonality_df = calculate_seasonality(monthly)
                seasonality_path = out_dir / "tourism_seasonality.csv"
                seasonality_df.to_csv(seasonality_path, index=False)
                print(f"✅ Wrote {seasonality_path}")

                seasonality_by_framework["tourism"] = seasonality_df

        results_by_framework[fw_name] = df_out

        # ==========================
        # CSV export
        # ==========================
        csv_path = out_dir / f"{fw_name}_macro_long.csv"
        df_out.to_csv(csv_path, index=False)
        print(f"✅ Wrote {csv_path}")

        # ==========================
        # XLSX export (by indicator)
        # ==========================
        xlsx_path = out_dir / f"{fw_name}_views_by_indicator.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="raw_long", index=False)

            for ind_name in sorted(df_out["indicator"].dropna().unique()):
                sub = df_out[df_out["indicator"] == ind_name].copy()
                if sub.empty:
                    continue

                if "month" in sub.columns and sub["month"].notna().any():
                    sub["period"] = (
                        sub["date"].astype(int).astype(str)
                        + "-"
                        + sub["month"].astype(int).astype(str).str.zfill(2)
                    )
                    col_field = "period"
                else:
                    col_field = "date"

                if "sub_indicator_short" in sub.columns and sub["sub_indicator_short"].notna().any():
                    table = (
                        sub.pivot_table(
                            index=["geo", "sub_indicator_short"],
                            columns=col_field,
                            values="value",
                            aggfunc="first",
                        )
                        .reset_index()
                    )
                else:
                    table = (
                        sub.pivot_table(
                            index=["geo"],
                            columns=col_field,
                            values="value",
                            aggfunc="first",
                        )
                        .reset_index()
                    )

                table.to_excel(writer, sheet_name=ind_name[:31], index=False)

        print(f"✅ Wrote {xlsx_path}")

    # ==========================
    # AI: Demographics
    # ==========================
    try:
        prompts = load_prompts()
        demo_prompt = prompts["demographics_executive_narrative"]["prompt"]

        ai_out_dir = out_dir / "demographics_executive_briefings"
        ai_out_dir.mkdir(exist_ok=True, parents=True)

        df_demo = results_by_framework.get("demographics")
        if df_demo is None or df_demo.empty:
            print("⚠️ Demographics AI skipped (no demographics data).")
        else:
            for geo in geos:
                briefing = generate_demographics_briefing(df_demo, geo, demo_prompt)
                (ai_out_dir / f"{geo}_executive_briefing.txt").write_text(briefing, encoding="utf-8")
            print("✅ Generated demographics AI executive briefings")

    except Exception as e:
        print(f"⚠️ Demographics AI generation failed: {type(e).__name__}: {e}")

    # ==========================
    # AI: Tourism
    # ==========================
    try:
        prompts = load_prompts()
        tour_prompt = prompts["tourism_executive_narrative"]["prompt"]

        ai_out_dir = out_dir / "tourism_executive_briefings"
        ai_out_dir.mkdir(exist_ok=True, parents=True)

        df_tour = results_by_framework.get("tourism")
        if df_tour is None or df_tour.empty:
            print("⚠️ Tourism AI skipped (no tourism data).")
        else:
            seasonality_df = seasonality_by_framework.get("tourism")
            for geo in geos:
                briefing = generate_tourism_briefing(df_tour, geo, tour_prompt, seasonality_df=seasonality_df)
                (ai_out_dir / f"{geo}_executive_briefing.txt").write_text(briefing, encoding="utf-8")
            print("✅ Generated tourism AI executive briefings")

    except Exception as e:
        print(f"⚠️ Tourism AI generation failed: {type(e).__name__}: {e}")

    # ==========================
    # AI: Economics
    # ==========================
    try:
        prompts = load_prompts()
        econ_prompt = prompts["economics_executive_narrative"]["prompt"]

        ai_out_dir = out_dir / "economics_executive_briefings"
        ai_out_dir.mkdir(exist_ok=True, parents=True)

        df_econ = results_by_framework.get("economics")
        if df_econ is None or df_econ.empty:
            print("⚠️ Economics AI skipped (no economics data).")
        else:
            for geo in geos:
                briefing = generate_economics_briefing(df_econ, geo, econ_prompt)
                (ai_out_dir / f"{geo}_executive_briefing.txt").write_text(
                    briefing, encoding="utf-8"
                )
            print("✅ Generated economics AI executive briefings")

    except Exception as e:
        print(f"⚠️ Economics AI generation failed: {type(e).__name__}: {e}")

    # ==========================
    # Single-sheet workbook
    # ==========================
    single_path = out_dir / "views_single_sheet.xlsx"
    build_views_single_sheet_workbook(results_by_framework, single_path, space_rows=3)
    print(f"✅ Wrote {single_path}")


if __name__ == "__main__":
    main()