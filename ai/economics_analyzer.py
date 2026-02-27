# ai/economics_analyzer.py
from __future__ import annotations

import pandas as pd

from ai.mistral_client import generate_text


def build_structured_data_block(df_country: pd.DataFrame) -> str:
    lines = []

    for indicator in sorted(df_country["indicator"].unique()):
        sub = df_country[df_country["indicator"] == indicator]
        lines.append(f"\nIndicator: {indicator}")

        for _, row in sub.sort_values(["date", "month"] if "month" in sub.columns else ["date"]).iterrows():
            period = str(row["date"])
            if "month" in sub.columns and pd.notna(row.get("month")):
                period = f"{int(row['date'])}-{int(row['month']):02d}"

            lines.append(f"{period} | {row['value']}")

    return "\n".join(lines)


def generate_economics_briefing(
    df: pd.DataFrame,
    geo: str,
    base_prompt: str,
) -> str:
    df_country = df[df["geo"] == geo].copy()
    data_block = build_structured_data_block(df_country)

    full_prompt = f"{base_prompt}\n\nSTRUCTURED DATA:\n{data_block}"

    return generate_text(full_prompt)