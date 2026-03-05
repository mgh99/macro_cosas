# ai/concentrated_overview_analyzer.py
from __future__ import annotations

import pandas as pd

from ai.mistral_client import generate_text


def build_structured_data_block(df: pd.DataFrame) -> str:
    """
    Convierte un DataFrame "long" (geo/indicator/date/value/...) en un bloque de texto
    estructurado (tipo CSV) para pasarlo a un LLM dentro del prompt.

    Formato final (por cada indicador):
      Indicator: <indicator_name>
      series,<col1>,<col2>,...
      <series_name>,<val>,<val>,...

    - Si el dataset es mensual (columna 'month' con valores), genera period = "YYYY-MM".
    - Si NO es mensual, usa 'date' como columna temporal.
    - Si falta 'sub_indicator_short', se usa "TOTAL" como única serie.

    Este bloque está diseñado para:
    - mantener el prompt estable
    - permitir al modelo “leer” tablas de forma clara y repetible

    Returns:
        str: Texto listo para incrustar dentro del prompt como "STRUCTURED DATA".
    """
    if df is None or df.empty:
        return "NO DATA."

    # Nota: asumimos que siempre existe df["indicator"] en los resultados del engine.
    lines: list[str] = []

    # Recorremos indicadores en orden alfabético para que el bloque sea determinista
    for indicator in sorted(df["indicator"].dropna().unique()):
        sub = df[df["indicator"] == indicator].copy()

        # ----
        # Definimos el eje temporal:
        # - Si hay 'month' → construimos "YYYY-MM" (period)
        # - Si no → usamos 'date' (normalmente "YYYY")
        # ----
        if "month" in sub.columns and sub["month"].notna().any():
            sub["period"] = (
                sub["date"].astype(int).astype(str)
                + "-"
                + sub["month"].astype(int).astype(str).str.zfill(2)
            )
            col_field = "period"

            # Orden: primero por serie, luego por tiempo
            # (para que pivot_table se construya de forma consistente)
            if "sub_indicator_short" in sub.columns:
                sub = sub.sort_values(["sub_indicator_short", "date", "month"])
            else:
                sub = sub.sort_values(["date", "month"])
        else:
            col_field = "date"
            if "sub_indicator_short" in sub.columns:
                sub = sub.sort_values(["sub_indicator_short", "date"])
            else:
                sub = sub.sort_values(["date"])

        # ----
        # Si no hay sub-indicador, creamos una serie TOTAL para no romper el pivot
        # ----
        if "sub_indicator_short" not in sub.columns:
            sub["sub_indicator_short"] = "TOTAL"

        # ----
        # Pivot: filas = series, columnas = tiempo, valores = value
        # ----
        table = (
            sub.pivot_table(
                index="sub_indicator_short",
                columns=col_field,
                values="value",
                aggfunc="first",  # si hay duplicados, nos quedamos con el primero
            )
            .reset_index()
            .rename(columns={"sub_indicator_short": "series"})
        )

        # ----
        # Serializamos como CSV simple:
        # 1) cabecera con Indicator
        # 2) fila header (nombres columnas)
        # 3) filas de datos
        # ----
        lines.append(f"\nIndicator: {indicator}")
        lines.append(",".join(map(str, table.columns)))

        for _, r in table.iterrows():
            row_vals = [r.get(c, "") for c in table.columns]
            # Reemplazamos NaN por vacío para que el bloque no se llene de "nan"
            lines.append(",".join("" if pd.isna(x) else str(x) for x in row_vals))

    return "\n".join(lines)


def generate_concentrated_overview(
    df_econ: pd.DataFrame,
    df_tour: pd.DataFrame,
    df_demo: pd.DataFrame,
    geo: str,
    base_prompt: str,
) -> str:
    """
    Genera un "one-slide / concentrated overview" para un país concreto (geo),
    combinando economics + tourism + demographics en un único bloque de datos.

    Args:
        df_econ, df_tour, df_demo:
            DataFrames de cada framework en formato long, típicamente con columnas:
            ['geo','indicator','date','value', ...]
        geo:
            Código ISO2 (ej: "ES") en world/europe frameworks de país.
            (OJO: para NUTS3 sería diferente, pero aquí se usa para el overview global.)
        base_prompt:
            Prompt de sistema (texto) definido en config/prompts.yaml.

    Returns:
        str: Texto generado por el modelo (Mistral) para pegar directamente en un slide.
    """
    # Filtramos por geo para no pasarle al LLM datos de otros países
    econ = df_econ[df_econ["geo"] == geo].copy() if df_econ is not None else pd.DataFrame()
    tour = df_tour[df_tour["geo"] == geo].copy() if df_tour is not None else pd.DataFrame()
    demo = df_demo[df_demo["geo"] == geo].copy() if df_demo is not None else pd.DataFrame()

    # Unificamos todo en una sola tabla long
    df_all = pd.concat([econ, tour, demo], ignore_index=True)

    # Generamos el bloque estructurado que verá el modelo
    data_block = build_structured_data_block(df_all)

    # Prompt final
    full_prompt = f"{base_prompt}\n\nSTRUCTURED DATA:\n{data_block}"

    # Llamada al modelo
    return generate_text(full_prompt)