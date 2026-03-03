# ai/concentrated_overview_analyzer.py
from __future__ import annotations

import pandas as pd

from ai.mistral_client import generate_text


def build_structured_data_block(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "NO DATA."

    lines: list[str] = []
    for indicator in sorted(df["indicator"].dropna().unique()):
        sub = df[df["indicator"] == indicator].copy()

        if "month" in sub.columns and sub["month"].notna().any():
            sub["period"] = (
                sub["date"].astype(int).astype(str)
                + "-"
                + sub["month"].astype(int).astype(str).str.zfill(2)
            )
            col_field = "period"
            sub = sub.sort_values(["sub_indicator_short", "date", "month"])
        else:
            col_field = "date"
            sub = sub.sort_values(["sub_indicator_short", "date"])

        if "sub_indicator_short" not in sub.columns:
            sub["sub_indicator_short"] = "TOTAL"

        table = (
            sub.pivot_table(
                index="sub_indicator_short",
                columns=col_field,
                values="value",
                aggfunc="first",
            )
            .reset_index()
            .rename(columns={"sub_indicator_short": "series"})
        )

        lines.append(f"\nIndicator: {indicator}")
        lines.append(",".join(map(str, table.columns)))
        for _, r in table.iterrows():
            row_vals = [r.get(c, "") for c in table.columns]
            lines.append(",".join("" if pd.isna(x) else str(x) for x in row_vals))

    return "\n".join(lines)


def generate_concentrated_overview(
    df_econ: pd.DataFrame,
    df_tour: pd.DataFrame,
    df_demo: pd.DataFrame,
    geo: str,
    base_prompt: str,
) -> str:
    econ = df_econ[df_econ["geo"] == geo].copy() if df_econ is not None else pd.DataFrame()
    tour = df_tour[df_tour["geo"] == geo].copy() if df_tour is not None else pd.DataFrame()
    demo = df_demo[df_demo["geo"] == geo].copy() if df_demo is not None else pd.DataFrame()

    df_all = pd.concat([econ, tour, demo], ignore_index=True)
    data_block = build_structured_data_block(df_all)

    full_prompt = f"{base_prompt}\n\nSTRUCTURED DATA:\n{data_block}"
    return generate_text(full_prompt)