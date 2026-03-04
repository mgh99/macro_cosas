# core/excel_single_sheet.py
from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


def _write_single_sheet(
    df_out: pd.DataFrame,
    writer: pd.ExcelWriter,
    sheet_name: str,
    space_rows: int = 3,
) -> bool:
    """Devuelve True si escribió algo en la hoja, False si no."""
    if df_out is None or df_out.empty:
        return False
    if "indicator" not in df_out.columns:
        raise ValueError(f"DataFrame for '{sheet_name}' is missing required column: 'indicator'")

    ws_row = 0
    wrote_any = False

    if "indicator_order" in df_out.columns and df_out["indicator_order"].notna().any():
        indicator_names = (
            df_out[["indicator", "indicator_order"]]
            .dropna()
            .drop_duplicates()
            .sort_values("indicator_order")
        )["indicator"].tolist()
    else:
        indicator_names = sorted(df_out["indicator"].dropna().unique())

    for ind_name in indicator_names:
        sub = df_out[df_out["indicator"] == ind_name].copy()
        if sub.empty:
            continue

        wrote_any = True

        # Title row
        title_df = pd.DataFrame([[ind_name]], columns=[""])
        title_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=ws_row)
        ws_row += 2

        # ✅ si es mensual, pivot por YYYY-MM
        if "month" in sub.columns and sub["month"].notna().any():
            sub = sub.copy()
            sub["period"] = (
                sub["date"].astype(int).astype(str)
                + "-"
                + sub["month"].astype(int).astype(str).str.zfill(2)
            )
            col_field = "period"
        else:
            col_field = "date"

        # Pivot
        geo_col = "geo_name" if "geo_name" in sub.columns and sub["geo_name"].notna().any() else "geo"

        if "sub_indicator_short" in sub.columns and sub["sub_indicator_short"].notna().any():
            table = (
                sub.pivot_table(
                    index=[geo_col, "sub_indicator_short"],
                    columns=col_field,
                    values="value",
                    aggfunc="first",
                )
                .reset_index()
                .rename(columns={"sub_indicator_short": "series"})
            )
        else:
            table = (
                sub.pivot_table(
                    index=[geo_col],
                    columns=col_field,
                    values="value",
                    aggfunc="first",
                )
                .reset_index()
            )

        table.to_excel(writer, sheet_name=sheet_name, index=False, startrow=ws_row)
        ws_row += len(table) + 1 + space_rows

    return wrote_any


def build_views_single_sheet_workbook(framework_dfs: Dict[str, pd.DataFrame], out_path: Path, space_rows: int = 3) -> None:
    """
    Crea 1 XLSX con N pestañas, una por framework:
      - demographics
      - tourism
      - etc.
    """
    non_empty = {k: v for k, v in (framework_dfs or {}).items() if v is not None and not v.empty}
    if not non_empty:
        print("⚠️ views_single_sheet.xlsx skipped (no framework data).")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    wrote_sheets = 0
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for framework_name, df_out in non_empty.items():
            try:
                if _write_single_sheet(df_out, writer, sheet_name=framework_name[:31], space_rows=space_rows):
                    wrote_sheets += 1
            except Exception as e:
                print(f"⚠️ Skipped sheet '{framework_name}' due to error: {type(e).__name__}: {e}")

        # Si por lo que sea no se escribió ninguna hoja, crea una “README” visible
        if wrote_sheets == 0:
            pd.DataFrame([["No framework views could be generated. Check logs."]]).to_excel(
                writer, sheet_name="README", index=False, header=False
            )