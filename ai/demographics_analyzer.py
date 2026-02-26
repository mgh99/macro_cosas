# ai/demographics_analyzer.py
from pathlib import Path

import pandas as pd

from ai.mistral_client import generate_text


def build_structured_data_block(df_country: pd.DataFrame) -> str:
    lines = []

    for indicator in df_country["indicator"].unique():
        sub = df_country[df_country["indicator"] == indicator]
        lines.append(f"\nIndicator: {indicator}")
        for _, row in sub.iterrows():
            lines.append(
                f"{row['date']} | {row.get('sub_indicator_short', '')} | {row['value']}"
            )

    return "\n".join(lines)


def generate_demographics_briefing(df: pd.DataFrame, geo: str, base_prompt: str) -> str:
    df_country = df[df["geo"] == geo].copy()
    data_block = build_structured_data_block(df_country)

    full_prompt = f"{base_prompt}\n\nSTRUCTURED DATA:\n{data_block}"

    return generate_text(full_prompt)