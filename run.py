# run.py
from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from ai.concentrated_overview_analyzer import generate_concentrated_overview
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


def _run_with_backoff(fn, max_tries: int = 6):
    delay = 2
    for _attempt in range(max_tries):
        try:
            return fn()
        except Exception as e:
            msg = str(e)
            if "429" in msg or "rate limit" in msg.lower():
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise
    raise RuntimeError("Rate limit persists after retries.")

def run_engine(
    geos: List[str],
    selected_frameworks: Optional[List[str]] = None,
    output_dir: Path | str = Path("outputs"),
    enable_ai: bool = True,
    output_flags: Optional[Dict[str, bool]] = None,
    debug_describe_eurostat: bool = False,
    frameworks_path: str = "config/frameworks.yaml",
    prompts_path: str = "config/prompts.yaml",
) -> Dict[str, pd.DataFrame]:
    """
    Main macro engine callable from CLI.

    - geos: ISO2 list like ["ES","FR","DE"]
    - selected_frameworks: subset of frameworks keys (or None = all)
    - output_dir: where to write CSV/XLSX/AI
    - enable_ai: whether to generate AI briefings
    - output_flags: {"csv": bool, "excel_by_indicator": bool, "single_sheet": bool, "debug_no_files": bool}
    - debug_describe_eurostat: prints dataset dimensions (slow-ish)
    - frameworks_path: which frameworks YAML to load (profile-specific)
    - prompts_path: which prompts YAML to load (profile-specific)

    Returns: results_by_framework dict
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    flags = output_flags or {
        "csv": True,
        "excel_by_indicator": True,
        "single_sheet": True,
        "debug_no_files": False,
    }

    # ✅ load frameworks from profile path
    cfg = load_config(frameworks_path)
    frameworks_all = cfg.get("frameworks", {}) or {}

    # Filter frameworks if user selected subset
    if selected_frameworks:
        wanted = set(selected_frameworks)
        frameworks = {k: v for k, v in frameworks_all.items() if k in wanted}
    else:
        frameworks = frameworks_all

    if not frameworks:
        raise ValueError("No frameworks selected / found.")

    # ==========================
    # Optional: describe Eurostat datasets (debug)
    # ==========================
    if debug_describe_eurostat:
        seen = set()
        for fw in frameworks.values():
            for ind in fw.get("indicators", []):
                if not ind.get("enabled", True):
                    continue
                source = (ind.get("source") or "eurostat").lower()
                if source != "eurostat":
                    continue
                ds = ind.get("dataset")
                if not ds or ds in seen:
                    continue
                describe_dataset(ds, sample_geo=geos[0], overrides=ind.get("describe_overrides"))
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

        # check debug
        #print(f"DEBUG {fw_name}: df_out rows={len(df_out)} cols={list(df_out.columns)}")
        #print(df_out.head(10))
        #print(df_out["indicator"].value_counts(dropna=False).head(20))

        # IMPORTANT: store results for AI + single-sheet
        results_by_framework[fw_name] = df_out

        # ==========================
        # Tourism extras
        # ==========================

        # Tourism extras: Top origins (only if indicator exists)
        if fw_name == "tourism" and any(
            i.get("name") == "arrivals_by_origin_hotels_number"
            for i in fw.get("indicators", [])
        ):
            ind_1b = "arrivals_by_origin_hotels_number"
            df_1b = df_out[df_out["indicator"] == ind_1b].copy()

            if df_1b.empty or "sub_indicator_short" not in df_1b.columns:
                print("⚠️ Top origins skipped: sub_indicator_short missing (did you preserve c_resid?)")
            else:
                exclude = {"TOTAL", "EU27_2020", "EA20", "EA19", "EU28"}
                df_1b = df_1b[~df_1b["sub_indicator_short"].isin(exclude)]

                top10 = top_origins(df_1b, top_n=10)
                outp = out_dir / "tourism_arrivals_top10_origins.csv"
                if not flags.get("debug_no_files", False) and flags.get("csv", True):
                    top10.to_csv(outp, index=False)
                    print(f"✅ Wrote {outp}")

            # Tourism extras: Seasonality (only if monthly indicator exists)
            if fw_name == "tourism" and any(
                i.get("name") == "nights_spent_monthly_hotels"
                for i in fw.get("indicators", [])
            ):
                monthly_indicator_name = "nights_spent_monthly_hotels"
                monthly = df_out[df_out["indicator"] == monthly_indicator_name].copy()

                if monthly.empty:
                    print("⚠️ Seasonality skipped (monthly indicator not found).")
                else:
                    seasonality_df = calculate_seasonality(monthly)
                    seasonality_by_framework["tourism"] = seasonality_df

                    seasonality_path = out_dir / "tourism_seasonality.csv"
                    if not flags.get("debug_no_files", False) and flags.get("csv", True):
                        seasonality_df.to_csv(seasonality_path, index=False)
                        print(f"✅ Wrote {seasonality_path}")

        # ==========================
        # CSV export
        # ==========================
        if not flags.get("debug_no_files", False) and flags.get("csv", True):
            csv_path = out_dir / f"{fw_name}_macro_long.csv"
            df_out.to_csv(csv_path, index=False)
            print(f"✅ Wrote {csv_path}")

        # ==========================
        # XLSX export (by indicator)
        # ==========================
        if not flags.get("debug_no_files", False) and flags.get("excel_by_indicator", True):
            xlsx_path = out_dir / f"{fw_name}_views_by_indicator.xlsx"
            with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                df_out.to_excel(writer, sheet_name="raw_long", index=False)
                used_sheet_names = {"raw_long"}

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

                    base_sheet = str(ind_name)[:31]
                    sheet_name = base_sheet
                    i = 2
                    while sheet_name in used_sheet_names:
                        suffix = f"_{i}"
                        sheet_name = f"{base_sheet[:31-len(suffix)]}{suffix}"
                        i += 1
                    used_sheet_names.add(sheet_name)
                    table.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"✅ Wrote {xlsx_path}")

    # ==========================
    # AI generation (optional)
    # ==========================
    if enable_ai and not flags.get("debug_no_files", False):
        prompts = load_prompts(prompts_path)

        def _get_prompt(key: str) -> str | None:
            obj = prompts.get(key)
            if isinstance(obj, dict):
                p = obj.get("prompt")
                return p if isinstance(p, str) and p.strip() else None
            return None

        # Only generate AI for frameworks that exist in results
        frameworks_ran = set(results_by_framework.keys())

        # Demographics
        if "demographics" in frameworks_ran:
            demo_prompt = _get_prompt("demographics_executive_narrative")
            if not demo_prompt:
                print("ℹ️ Demographics AI skipped (prompt not found in this profile).")
            else:
                try:
                    ai_out_dir = out_dir / "demographics_executive_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_demo = results_by_framework.get("demographics")
                    if df_demo is None or df_demo.empty:
                        print("ℹ️ Demographics AI skipped (no demographics data).")
                    else:
                        for geo in geos:
                            briefing = generate_demographics_briefing(df_demo, geo, demo_prompt)
                            (ai_out_dir / f"{geo}_executive_briefing.txt").write_text(briefing, encoding="utf-8")
                        print("✅ Generated demographics AI executive briefings")
                except Exception as e:
                    print(f"⚠️ Demographics AI generation failed: {type(e).__name__}: {e}")

        # Tourism
        if "tourism" in frameworks_ran:
            tour_prompt = _get_prompt("tourism_executive_narrative")
            if not tour_prompt:
                print("ℹ️ Tourism AI skipped (prompt not found in this profile).")
            else:
                try:
                    ai_out_dir = out_dir / "tourism_executive_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_tour = results_by_framework.get("tourism")
                    if df_tour is None or df_tour.empty:
                        print("ℹ️ Tourism AI skipped (no tourism data).")
                    else:
                        seasonality_df = seasonality_by_framework.get("tourism")
                        for geo in geos:
                            briefing = generate_tourism_briefing(df_tour, geo, tour_prompt, seasonality_df=seasonality_df)
                            (ai_out_dir / f"{geo}_executive_briefing.txt").write_text(briefing, encoding="utf-8")
                        print("✅ Generated tourism AI executive briefings")
                except Exception as e:
                    print(f"⚠️ Tourism AI generation failed: {type(e).__name__}: {e}")

        # Economics
        if "economics" in frameworks_ran:
            eco_prompt = _get_prompt("economics_executive_narrative")
            if not eco_prompt:
                print("ℹ️ Economics AI skipped (prompt not found in this profile).")
            else:
                try:
                    ai_out_dir = out_dir / "economics_executive_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_eco = results_by_framework.get("economics")
                    if df_eco is None or df_eco.empty:
                        print("ℹ️ Economics AI skipped (no economics data).")
                    else:
                        for geo in geos:
                            briefing = _run_with_backoff(
                                lambda: generate_economics_briefing(df_eco, geo, eco_prompt)
                            )
                            (ai_out_dir / f"{geo}_executive_briefing.txt").write_text(
                                briefing, encoding="utf-8"
                            )
                        print("✅ Generated economics AI executive briefings")
                except Exception as e:
                    print(f"⚠️ Economics AI generation failed: {type(e).__name__}: {e}")

        # Concentrated Overview (requires economics + tourism + demographics)
        if {"economics", "tourism", "demographics"}.issubset(frameworks_ran):
            one_slide_prompt = _get_prompt("concentrated_overview_executive_narrative")
            if not one_slide_prompt:
                print("ℹ️ Concentrated overview AI skipped (prompt not found in this profile).")
            else:
                try:
                    ai_out_dir = out_dir / "concentrated_overview_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_eco = results_by_framework.get("economics")
                    df_tour = results_by_framework.get("tourism")
                    df_demo = results_by_framework.get("demographics")

                    if any(df is None or df.empty for df in [df_eco, df_tour, df_demo]):
                        print("ℹ️ Concentrated overview skipped (missing framework data).")
                    else:
                        for geo in geos:
                            briefing = generate_concentrated_overview(
                                df_econ=df_eco,
                                df_tour=df_tour,
                                df_demo=df_demo,
                                geo=geo,
                                base_prompt=one_slide_prompt,
                            )
                            (ai_out_dir / f"{geo}_one_slide.txt").write_text(
                                briefing,
                                encoding="utf-8",
                            )
                        print("✅ Generated concentrated overview one-slide briefings")
                except Exception as e:
                    print(f"⚠️ Concentrated overview AI generation failed: {type(e).__name__}: {e}")

    # ==========================
    # Single-sheet workbook
    # ==========================
    if not flags.get("debug_no_files", False) and flags.get("single_sheet", True):
        single_path = out_dir / "views_single_sheet.xlsx"
        build_views_single_sheet_workbook(results_by_framework, single_path, space_rows=3)
        print(f"✅ Wrote {single_path}")

    return results_by_framework


def main() -> None:
    """
    Non-interactive default run (kept for dev use).
    For interactive usage: run `python cli_menu.py`.
    """
    geos = ["ES", "FR", "DE"]
    run_engine(
        geos=geos,
        selected_frameworks=None,
        output_dir=Path("outputs"),
        enable_ai=True,
        frameworks_path="config/frameworks.yaml",
        prompts_path="config/prompts.yaml",
    )


if __name__ == "__main__":
    main()
