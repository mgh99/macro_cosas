from __future__ import annotations

import pandas as pd

from ai.mistral_client import generate_text


def build_structured_data_block(df_country: pd.DataFrame) -> str:
    """
    Formato robusto para anual y mensual.
    Produce una tabla por indicador:
      series | 2021 | 2022 | ...
    o si mensual:
      series | 2024-01 | 2024-02 | ...
    """
    if df_country is None or df_country.empty:
        return "NO DATA FOR THIS COUNTRY."

    lines: list[str] = []

    for indicator in sorted(df_country["indicator"].dropna().unique()):
        sub = df_country[df_country["indicator"] == indicator].copy()

        # Decide columnas: anual vs mensual
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

        # Asegura que exista 'sub_indicator_short' (si no, pon un default)
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

        # Render simple “tipo CSV” dentro del prompt
        lines.append(f"\nIndicator: {indicator}")
        lines.append(",".join(map(str, table.columns)))

        for _, r in table.iterrows():
            row_vals = [r.get(c, "") for c in table.columns]
            lines.append(",".join("" if pd.isna(x) else str(x) for x in row_vals))

    return "\n".join(lines)


def build_seasonality_block(seasonality_df: pd.DataFrame) -> str:
    if seasonality_df is None or seasonality_df.empty:
        return "NO SEASONALITY DATA."

    cols = list(seasonality_df.columns)
    lines = [
        "SEASONALITY KPI (Top3 months nights / annual total):",
        f"Columns: {cols}",
    ]
    for _, r in seasonality_df.iterrows():
        geo = r.get("geo", "")
        year = r.get("year", r.get("date", ""))
        ratio = r.get("seasonality_ratio", r.get("ratio", ""))
        cls = r.get("seasonality_class", r.get("class", ""))
        lines.append(f"{geo} | {year} | ratio={ratio} | class={cls}")
    return "\n".join(lines)


def generate_tourism_briefing(
    df: pd.DataFrame,
    geo: str,
    base_prompt: str,
    seasonality_df: pd.DataFrame | None = None,
) -> str:
    df_country = df[df["geo"] == geo].copy()
    data_block = build_structured_data_block(df_country)

    seasonality_block = ""
    if seasonality_df is not None and not seasonality_df.empty:
        if "geo" in seasonality_df.columns:
            seasonality_country = seasonality_df[seasonality_df["geo"] == geo]
        else:
            seasonality_country = seasonality_df
        seasonality_block = "\n\n" + build_seasonality_block(seasonality_country)

    full_prompt = f"{base_prompt}\n\nSTRUCTURED DATA:\n{data_block}{seasonality_block}"
    return generate_text(full_prompt)