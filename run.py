# run.py
from __future__ import annotations

"""
Macro Strategy Engine (runner)

This module is the "orchestrator":
- Loads the selected profile frameworks (YAML)
- Fetches data indicator-by-indicator (Eurostat / OECD / IMF / etc.)
- Exports results (CSV / Excel / single-sheet workbook)
- Optionally runs AI narratives using the prepared prompts

Non-technical usage:
- Usually you run this via: python cli_menu.py
- The only thing most users should edit is the YAML in config/profiles/*

Common gotchas:
- Excel sheet names are limited to 31 characters, so long indicator names get truncated.
- The "nuts3" framework ONLY runs if nuts3_geos are provided (e.g. ES300).
"""

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
from core.country_resolver import AGGREGATE_GEO_CODES
from core.data_fetcher import fetch_indicator_for_geo, fetch_indicator_for_geos
from core.excel_single_sheet import build_views_single_sheet_workbook
from core.prompt_loader import load_prompts
from core.seasonality import calculate_seasonality
from core.top_origins import top_origins


def _run_with_backoff(fn, max_tries: int = 6):
    """
    Retry helper for rate limits (mainly used for AI calls).
    If the provider returns 429 / rate limit, we retry with exponential backoff.
    """
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
    nuts3_geos: Optional[List[str]] = None,
    selected_frameworks: Optional[List[str]] = None,
    output_dir: Path | str = Path("outputs"),
    enable_ai: bool = True,
    output_flags: Optional[Dict[str, bool]] = None,
    debug_describe_eurostat: bool = False,
    frameworks_path: str = "config/frameworks.yaml",
    prompts_path: str = "config/prompts.yaml",
    progress_callback=None,
) -> Dict[str, pd.DataFrame]:
    """
    Main engine callable from CLI.

    Inputs
    - geos: list of ISO2 country codes, e.g. ["ES","FR"]
    - nuts3_geos: list of NUTS3 region codes, e.g. ["ES300"]
    - selected_frameworks: subset of framework keys to run (None = run all)
    - output_dir: output folder
    - enable_ai: whether to generate AI executive narratives
    - output_flags:
        csv: write long CSV per framework
        excel_by_indicator: write an Excel with 1 tab per indicator
        single_sheet: write 1 combined workbook (nice for human reading)
        debug_no_files: do not write files (console run only)
    - debug_describe_eurostat: print Eurostat dataset dimension codes (for debugging filters)
    - frameworks_path / prompts_path: which profile files to load

    Returns
    - results_by_framework: dict {framework_name: df_long}
      Each df is in "long format": geo | indicator | date | value | ...
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    flags = output_flags or {
        "csv": True,
        "excel_by_indicator": True,
        "single_sheet": True,
        "debug_no_files": False,
    }

    # --------------------------
    # Load profile frameworks (YAML)
    # --------------------------
    cfg = load_config(frameworks_path)
    frameworks_all = cfg.get("frameworks", {}) or {}

    # If the user selected a subset of frameworks, keep only those
    if selected_frameworks:
        wanted = set(selected_frameworks)
        frameworks = {k: v for k, v in frameworks_all.items() if k in wanted}
    else:
        frameworks = frameworks_all

    if not frameworks:
        raise ValueError("No frameworks selected / found.")

    # --------------------------
    # Optional: print Eurostat dimension codes (debug)
    # --------------------------
    if debug_describe_eurostat:
        seen = set()

        for fw_name, fw in frameworks.items():
            # For nuts3 datasets, we must use a NUTS3 geo code as sample_geo (e.g. ES300)
            if fw_name == "nuts3":
                sample_geo = (nuts3_geos[0] if nuts3_geos else None)
                if not sample_geo:
                    print("⚠️ Eurostat debug skipped for nuts3: no nuts3_geos provided.")
                    continue
            else:
                sample_geo = (geos[0] if geos else "ES")

            for ind in fw.get("indicators", []):
                if not ind.get("enabled", True):
                    continue
                source = (ind.get("source") or "eurostat").lower()
                if source != "eurostat":
                    continue

                ds = ind.get("dataset")
                if not ds or ds in seen:
                    continue

                describe_dataset(ds, sample_geo=sample_geo, overrides=ind.get("describe_overrides"))
                seen.add(ds)

    # --------------------------
    # Run frameworks
    # --------------------------
    results_by_framework: Dict[str, pd.DataFrame] = {}
    seasonality_by_framework: Dict[str, pd.DataFrame] = {}

    for fw_name, fw in frameworks.items():
        all_parts: List[pd.DataFrame] = []

        print(f"\n=== Framework: {fw_name} ===")

        # Decide which geos apply for this framework
        if fw_name == "nuts3":
            target_geos = nuts3_geos or []
            if not target_geos:
                print("⚠️ NUTS3 selected, but no NUTS3 regions were provided. Skipping nuts3 framework.")
                continue
        else:
            target_geos = geos

        # Fetch each indicator in this framework
        indicators_enabled = [i for i in fw.get("indicators", []) if i.get("enabled", True)]
        total_ind = len(indicators_enabled)
        for order_idx, ind in enumerate(indicators_enabled, start=1):
            source = (ind.get("source") or "eurostat").lower()
            ind_name = ind.get("name", "(unnamed_indicator)")
            ind_debug = bool(ind.get("debug", False))

            allow_agg = bool(ind.get("allow_aggregates", False))

            print(f"[{fw_name}] indicator {order_idx}/{total_ind}: {ind_name} ({source})")
            if progress_callback:
                progress_callback(fw_name, order_idx, total_ind, ind_name, source)

            # OECD connectors usually support multi-geo fetch in one go
            if source == "oecd":
                oecd_geos = target_geos if allow_agg else [g for g in target_geos if g not in AGGREGATE_GEO_CODES]
                if not oecd_geos:
                    continue
                df = fetch_indicator_for_geos(ind, oecd_geos)
                df["indicator_order"] = order_idx
                all_parts.append(df)

                if ind_debug:
                    print(f"🧪 DEBUG {fw_name}/{ind_name}: rows={len(df)} geos={oecd_geos}")

            else:
                # Most sources fetch geo-by-geo
                for geo_idx, geo in enumerate(target_geos, start=1):
                    if geo in AGGREGATE_GEO_CODES and not allow_agg:
                        continue
                    try:
                        df = fetch_indicator_for_geo(ind, geo)
                    except Exception as exc:
                        print(f"⚠️ {fw_name}/{ind_name}/{geo}: fetch failed ({exc}), skipping")
                        continue
                    df["indicator_order"] = order_idx
                    all_parts.append(df)

                    if ind_debug:
                        print(f"🧪 DEBUG {fw_name}/{ind_name}/{geo} ({geo_idx}/{len(target_geos)}): rows={len(df)}")

        if not all_parts:
            print(f"⚠️ No data fetched for framework: {fw_name}")
            continue

        # Standardize columns we keep for output + AI
        wanted_cols = ["geo", "indicator", "date", "month", "value", "unit", "sub_indicator_short", "indicator_order"]
        df_long = pd.concat(all_parts, ignore_index=True)
        df_out = df_long[[c for c in wanted_cols if c in df_long.columns]].copy()

        # Add human labels for NUTS3 codes (ES300 -> Madrid)
        if fw_name == "nuts3":
            from core.nuts3_resolver import nuts3_code_to_label_map
            m = nuts3_code_to_label_map()
            df_out["geo_name"] = df_out["geo"].map(m)

        # Keep for later AI + combined single-sheet workbook
        results_by_framework[fw_name] = df_out

        # --------------------------
        # Tourism extras (only for tourism framework)
        # --------------------------
        if fw_name == "tourism":
            # Top origins (requires indicator 1b)
            if any(i.get("name") == "arrivals_by_origin_hotels_number" for i in fw.get("indicators", [])):
                ind_1b = "arrivals_by_origin_hotels_number"
                df_1b = df_out[df_out["indicator"] == ind_1b].copy()

                if df_1b.empty or "sub_indicator_short" not in df_1b.columns:
                    print("⚠️ Top origins skipped: missing sub_indicator_short.")
                else:
                    exclude = {"TOTAL", "EU27_2020", "EA20", "EA19", "EU28"}
                    df_1b = df_1b[~df_1b["sub_indicator_short"].isin(exclude)]

                    top10 = top_origins(df_1b, top_n=10)
                    outp = out_dir / "tourism_arrivals_top10_origins.csv"
                    if not flags.get("debug_no_files", False) and flags.get("csv", True):
                        top10.to_csv(outp, index=False)
                        print(f"✅ Wrote {outp}")

            # Seasonality (requires monthly nights)
            if any(i.get("name") == "nights_spent_monthly_hotels" for i in fw.get("indicators", [])):
                monthly_indicator_name = "nights_spent_monthly_hotels"
                monthly = df_out[df_out["indicator"] == monthly_indicator_name].copy()

                if monthly.empty:
                    print("⚠️ Seasonality skipped: monthly indicator not found in output.")
                else:
                    seasonality_df = calculate_seasonality(monthly)
                    seasonality_by_framework["tourism"] = seasonality_df

                    seasonality_path = out_dir / "tourism_seasonality.csv"
                    if not flags.get("debug_no_files", False) and flags.get("csv", True):
                        seasonality_df.to_csv(seasonality_path, index=False)
                        print(f"✅ Wrote {seasonality_path}")

        # --------------------------
        # CSV export (long format)
        # --------------------------
        if not flags.get("debug_no_files", False) and flags.get("csv", True):
            csv_path = out_dir / f"{fw_name}_macro_long.csv"

            df_to_write = df_out
            if fw_name == "nuts3" and "geo_name" in df_out.columns:
                df_to_write = df_out.copy()
                df_to_write["geo_code"] = df_to_write["geo"]
                df_to_write["geo"] = df_to_write["geo_name"].fillna(df_to_write["geo"])

            df_to_write.to_csv(csv_path, index=False)
            print(f"✅ Wrote {csv_path}")

        # --------------------------
        # Excel export (one file per framework)
        # --------------------------
        if not flags.get("debug_no_files", False) and flags.get("excel_by_indicator", True):
            xlsx_path = out_dir / f"{fw_name}_views_by_indicator.xlsx"
            with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                # Raw long tab (always)
                df_out.to_excel(writer, sheet_name="raw_long", index=False)
                used_sheet_names = {"raw_long"}

                # Each indicator gets its own tab (Excel tab names are max 31 chars)
                for ind_name in sorted(df_out["indicator"].dropna().unique()):
                    sub = df_out[df_out["indicator"] == ind_name].copy()
                    if sub.empty:
                        continue

                    idx_geo = "geo"
                    if fw_name == "nuts3" and "geo_name" in sub.columns and sub["geo_name"].notna().any():
                        idx_geo = "geo_name"

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
                                index=[idx_geo, "sub_indicator_short"],
                                columns=col_field,
                                values="value",
                                aggfunc="first",
                            )
                            .reset_index()
                        )
                    else:
                        table = (
                            sub.pivot_table(
                                index=[idx_geo],
                                columns=col_field,
                                values="value",
                                aggfunc="first",
                            )
                            .reset_index()
                        )

                    base_sheet = str(ind_name)[:31]  # Excel limit
                    sheet_name = base_sheet
                    i = 2
                    while sheet_name in used_sheet_names:
                        suffix = f"_{i}"
                        sheet_name = f"{base_sheet[:31-len(suffix)]}{suffix}"
                        i += 1
                    used_sheet_names.add(sheet_name)

                    table.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"✅ Wrote {xlsx_path}")

    # --------------------------
    # AI generation (optional)
    # --------------------------
    if enable_ai and not flags.get("debug_no_files", False):
        prompts = load_prompts(prompts_path)

        def _get_prompt(key: str) -> str | None:
            obj = prompts.get(key)
            if isinstance(obj, dict):
                p = obj.get("prompt")
                return p if isinstance(p, str) and p.strip() else None
            return None

        frameworks_ran = set(results_by_framework.keys())

        # Demographics
        if "demographics" in frameworks_ran:
            demo_prompt = _get_prompt("demographics_executive_narrative")
            if not demo_prompt:
                print("ℹ️ Demographics AI skipped (prompt not found).")
            else:
                try:
                    ai_out_dir = out_dir / "demographics_executive_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_demo = results_by_framework.get("demographics")
                    if df_demo is None or df_demo.empty:
                        print("ℹ️ Demographics AI skipped (no data).")
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
                print("ℹ️ Tourism AI skipped (prompt not found).")
            else:
                try:
                    ai_out_dir = out_dir / "tourism_executive_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_tour = results_by_framework.get("tourism")
                    if df_tour is None or df_tour.empty:
                        print("ℹ️ Tourism AI skipped (no data).")
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
                print("ℹ️ Economics AI skipped (prompt not found).")
            else:
                try:
                    ai_out_dir = out_dir / "economics_executive_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_eco = results_by_framework.get("economics")
                    if df_eco is None or df_eco.empty:
                        print("ℹ️ Economics AI skipped (no data).")
                    else:
                        for geo in geos:
                            briefing = _run_with_backoff(lambda: generate_economics_briefing(df_eco, geo, eco_prompt))
                            (ai_out_dir / f"{geo}_executive_briefing.txt").write_text(briefing, encoding="utf-8")
                        print("✅ Generated economics AI executive briefings")
                except Exception as e:
                    print(f"⚠️ Economics AI generation failed: {type(e).__name__}: {e}")

        # One-slide overview (needs all 3)
        if {"economics", "tourism", "demographics"}.issubset(frameworks_ran):
            one_slide_prompt = _get_prompt("concentrated_overview_executive_narrative")
            if not one_slide_prompt:
                print("ℹ️ Concentrated overview AI skipped (prompt not found).")
            else:
                try:
                    ai_out_dir = out_dir / "concentrated_overview_briefings"
                    ai_out_dir.mkdir(exist_ok=True, parents=True)

                    df_eco = results_by_framework.get("economics")
                    df_tour = results_by_framework.get("tourism")
                    df_demo = results_by_framework.get("demographics")

                    if any(df is None or df.empty for df in [df_eco, df_tour, df_demo]):
                        print("ℹ️ Concentrated overview skipped (missing data).")
                    else:
                        for geo in geos:
                            briefing = generate_concentrated_overview(
                                df_econ=df_eco,
                                df_tour=df_tour,
                                df_demo=df_demo,
                                geo=geo,
                                base_prompt=one_slide_prompt,
                            )
                            (ai_out_dir / f"{geo}_one_slide.txt").write_text(briefing, encoding="utf-8")
                        print("✅ Generated concentrated overview one-slide briefings")
                except Exception as e:
                    print(f"⚠️ Concentrated overview AI generation failed: {type(e).__name__}: {e}")

    # --------------------------
    # Single-sheet workbook (one nice Excel with all frameworks)
    # --------------------------
    if not flags.get("debug_no_files", False) and flags.get("single_sheet", True):
        single_path = out_dir / "views_single_sheet.xlsx"
        build_views_single_sheet_workbook(results_by_framework, single_path, space_rows=3)
        print(f"✅ Wrote {single_path}")

    return results_by_framework

# Default non-interactive run (kept for dev use)
#def main() -> None:
#    """
#    Non-interactive default run (kept for dev use).
#    For interactive usage: run `python cli_menu.py`.
#    """
#    geos = ["ES", "FR", "DE"]
#    run_engine(
#        geos=geos,
#        selected_frameworks=None,
#        output_dir=Path("outputs"),
#        enable_ai=True,
#        frameworks_path="config/frameworks.yaml",
#        prompts_path="config/prompts.yaml",
#    )

# Dev quick test (kept small on purpose)
def main() -> None:
    run_engine(
        geos=["ES"], # España
        nuts3_geos=["ES300"], # Madrid NUTS3 code for testing (only if you have the nuts3 framework configured)
        selected_frameworks=["nuts3"],
        output_dir=Path("outputs"),
        enable_ai=False,
        debug_describe_eurostat=True,
        frameworks_path="config/profiles/world/frameworks.yaml",
        prompts_path="config/profiles/world/prompts.yaml",
        output_flags={"csv": False, "excel_by_indicator": False, "single_sheet": False, "debug_no_files": True},
    )


if __name__ == "__main__":
    main()