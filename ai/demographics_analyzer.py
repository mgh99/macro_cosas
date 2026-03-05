# ai/demographics_analyzer.py

import pandas as pd

from ai.mistral_client import generate_text


def build_structured_data_block(df_country: pd.DataFrame) -> str:
    """
    Convierte los datos demográficos de un país en un bloque de texto
    estructurado que será pasado al modelo de IA dentro del prompt.

    Formato generado:

        Indicator: population_by_age_group
        2019 | 0-14 | 15.3
        2019 | 15-24 | 10.2
        2020 | 0-14 | 15.1

    Cada línea representa:
        date | sub_indicator | value

    El objetivo es dar al modelo una representación clara y compacta
    de las series temporales para cada indicador.

    Args:
        df_country: DataFrame filtrado por país.

    Returns:
        str: Bloque de texto listo para incrustar en el prompt.
    """

    if df_country is None or df_country.empty:
        return "NO DATA."

    lines = []

    # Iteramos por indicador (ej: population_by_age_group)
    for indicator in df_country["indicator"].dropna().unique():
        sub = df_country[df_country["indicator"] == indicator]

        lines.append(f"\nIndicator: {indicator}")

        for _, row in sub.iterrows():
            lines.append(
                f"{row['date']} | {row.get('sub_indicator_short', '')} | {row['value']}"
            )

    return "\n".join(lines)


def generate_demographics_briefing(
    df: pd.DataFrame,
    geo: str,
    base_prompt: str,
) -> str:
    """
    Genera un briefing demográfico ejecutivo para un país.

    Flujo:
        1. Filtra el DataFrame por país (geo).
        2. Construye un bloque estructurado de datos.
        3. Inserta los datos dentro del prompt base.
        4. Envía el prompt al modelo (Mistral).

    Args:
        df:
            DataFrame con todos los países y todos los indicadores
            del framework demographics.

        geo:
            Código ISO2 del país (ej: "ES", "FR", "DE").

        base_prompt:
            Prompt base definido en config/prompts.yaml.

    Returns:
        str: Texto generado por el modelo (briefing demográfico).
    """

    # Filtramos solo el país solicitado
    df_country = df[df["geo"] == geo].copy()

    # Construimos el bloque de datos que verá el modelo
    data_block = build_structured_data_block(df_country)

    # Insertamos datos dentro del prompt
    full_prompt = f"{base_prompt}\n\nSTRUCTURED DATA:\n{data_block}"

    # Generamos el texto con el modelo
    return generate_text(full_prompt)