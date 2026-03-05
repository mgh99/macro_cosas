# ai/tourism_analyzer.py
from __future__ import annotations

import pandas as pd

from ai.mistral_client import generate_text


def build_structured_data_block(df_country: pd.DataFrame) -> str:
    """
    Convierte el DataFrame de un país (geo) en un bloque de texto compacto para el LLM.

    Objetivo:
      - Darle al modelo una “tabla” por indicador, con columnas por periodo.
      - Funciona para series anuales y mensuales.

    Formato aproximado en el prompt (tipo CSV):
      Indicator: nights_spent
      series,2021,2022,2023
      TOTAL,123,130,140
      FOR,80,90,95

    Si el indicador es mensual:
      series,2024-01,2024-02,2024-03
      TOTAL,10,12,15
    """
    if df_country is None or df_country.empty:
        return "NO DATA FOR THIS COUNTRY."

    lines: list[str] = []

    # Recorremos indicador por indicador para crear una mini-tabla por cada uno
    for indicator in sorted(df_country["indicator"].dropna().unique()):
        sub = df_country[df_country["indicator"] == indicator].copy()
        if sub.empty:
            continue

        # Aseguramos que exista la columna de series (sub-indicador).
        # Si el conector no la trae, asumimos una serie única "TOTAL".
        if "sub_indicator_short" not in sub.columns:
            sub["sub_indicator_short"] = "TOTAL"
        else:
            # Si existe pero viene todo vacío, también ponemos TOTAL
            if sub["sub_indicator_short"].isna().all():
                sub["sub_indicator_short"] = "TOTAL"

        # Detectamos si es mensual: si hay columna "month" con algún valor real
        is_monthly = "month" in sub.columns and sub["month"].notna().any()

        if is_monthly:
            # Construimos periodo YYYY-MM para pivotar
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

        # Pivot: filas = series (sub_indicator_short), columnas = periodos, valores = value
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

        # Escribimos la “tabla” dentro del prompt (como CSV simple)
        lines.append(f"\nIndicator: {indicator}")
        lines.append(",".join(map(str, table.columns)))

        for _, r in table.iterrows():
            row_vals = [r.get(c, "") for c in table.columns]
            # Convertimos NaN a vacío para que el prompt quede limpio
            lines.append(",".join("" if pd.isna(x) else str(x) for x in row_vals))

    return "\n".join(lines)


def build_seasonality_block(seasonality_df: pd.DataFrame) -> str:
    """
    Convierte el DataFrame de estacionalidad (calculado aparte) en texto legible para el LLM.

    Esperado (ideal):
      - geo
      - year
      - seasonality_ratio
      - seasonality_class
    """
    if seasonality_df is None or seasonality_df.empty:
        return "NO SEASONALITY DATA."

    # Para que el output sea estable
    df = seasonality_df.copy()
    sort_cols = [c for c in ["geo", "year"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    lines = ["SEASONALITY KPI (Top3 months nights / annual total):"]

    for _, r in df.iterrows():
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
    """
    Genera el briefing de turismo para un país (geo).

    Inputs:
      - df: datos largos (geo, indicator, date, value, etc.)
      - geo: ISO2 del país (ej: "ES") o el código que uses en tu pipeline
      - base_prompt: prompt del perfil (europe/world)
      - seasonality_df: opcional, calculado por calculate_seasonality()

    Output:
      - Texto final generado por el modelo (string)
    """
    # Filtramos el país
    df_country = df[df["geo"] == geo].copy()
    data_block = build_structured_data_block(df_country)

    # Si hay estacionalidad, la añadimos al final del prompt
    seasonality_block = ""
    if seasonality_df is not None and not seasonality_df.empty:
        if "geo" in seasonality_df.columns:
            seasonality_country = seasonality_df[seasonality_df["geo"] == geo].copy()
        else:
            # Si no hay columna geo (por algún motivo), usamos todo
            seasonality_country = seasonality_df.copy()

        seasonality_block = "\n\n" + build_seasonality_block(seasonality_country)

    full_prompt = f"{base_prompt}\n\nSTRUCTURED DATA:\n{data_block}{seasonality_block}"
    return generate_text(full_prompt)