# ai/economics_analyzer.py
from __future__ import annotations

"""
ECONOMICS ANALYZER

Este módulo genera el prompt final para el briefing económico (IA) a partir de
un DataFrame "long" del framework economics.

Qué hace:
- Filtra los datos por país (geo ISO2: "ES", "FR", etc.)
- Calcula un pequeño bloque de métricas resumen (latest, tendencia, CAGR…)
- Añade un bloque de series temporales truncadas (últimos N puntos)
- Monta el prompt final y llama al LLM (generate_text)

Columnas esperadas en el DataFrame de entrada (df):
- geo: str (ISO2)
- indicator: str (nombre interno del indicador)
- date: int o str convertible (año)
- month: int opcional (si serie mensual)
- value: numérico o convertible a numérico
- sub_indicator_short: opcional (no se usa aquí, pero puede existir)

Nota para Ana:
- Si quieres que el texto del briefing cambie, edita el "base_prompt" en YAML.
- Si quieres cambiar cuántos años/meses se pasan al modelo, cambia SeriesConfig.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd

from ai.mistral_client import generate_text


@dataclass
class SeriesConfig:
    """
    Configuración de "cuánto contexto" damos al modelo.
    Se recortan las series para que el prompt no explote en tokens.
    """
    max_points_monthly: int = 24   # últimos 24 meses
    max_points_annual: int = 12    # últimos 12 años
    round_decimals: int = 3        # redondeo de valores numéricos


# --- Helpers de formato y cálculos simples ---


def _is_monthly(sub: pd.DataFrame) -> bool:
    return "month" in sub.columns and sub["month"].notna().any()


def _period_str(row: pd.Series) -> str:
    """Devuelve 'YYYY' o 'YYYY-MM'."""
    y = int(row["date"])
    m = row.get("month", pd.NA)
    if pd.notna(m):
        return f"{y}-{int(m):02d}"
    return str(y)


def _safe_numeric_series(df_ind: pd.DataFrame) -> pd.Series:
    """
    Convierte 'value' a numérico y elimina valores no convertibles.
    """
    s = pd.to_numeric(df_ind["value"], errors="coerce")
    return s.dropna()


def _last_numeric_row(sub: pd.DataFrame) -> Optional[pd.Series]:
    """
    Devuelve la última fila con value numérico (no solo 'no-NaN').
    Evita petadas si llega algún string raro en 'value'.
    """
    tmp = sub.copy()
    tmp["_value_num"] = pd.to_numeric(tmp["value"], errors="coerce")
    tmp = tmp.dropna(subset=["_value_num"])
    if tmp.empty:
        return None
    return tmp.iloc[-1]


def _cagr(first: float, last: float, years: int) -> Optional[float]:
    """
    CAGR estándar. Solo aplica si first y last > 0.
    """
    if years <= 0 or first <= 0 or last <= 0:
        return None
    return (last / first) ** (1 / years) - 1


def _trend_direction(s: pd.Series, window: int = 6) -> str:
    """
    Tendencia simple: compara media de las últimas 'window' observaciones
    vs la media de las 'window' anteriores.
    """
    if len(s) < window * 2:
        return "insufficient_data"
    recent = s.iloc[-window:].mean()
    prev = s.iloc[-2 * window:-window].mean()
    if pd.isna(recent) or pd.isna(prev):
        return "insufficient_data"
    if recent > prev * 1.01:
        return "up"
    if recent < prev * 0.99:
        return "down"
    return "flat"


# --- Bloques de prompt ---


def build_summary_block(df_country: pd.DataFrame, cfg: SeriesConfig) -> str:
    """
    Bloque compacto con métricas derivadas EXCLUSIVAMENTE de los datos:
    - latest value (con periodo)
    - tendencia 6M si mensual
    - CAGR aproximado si anual y hay histórico suficiente
    - spread (Top10 - Bottom10) si ambos indicadores existen
    """
    if df_country is None or df_country.empty:
        return "SUMMARY METRICS:\n- NO DATA."

    lines = ["SUMMARY METRICS (computed only from provided data):"]

    # guardamos últimos valores por indicador para cálculos cruzados
    latest_by_indicator: Dict[str, Tuple[str, float]] = {}

    for indicator in sorted(df_country["indicator"].dropna().unique()):
        sub = df_country[df_country["indicator"] == indicator].copy()
        if sub.empty:
            continue

        # ordenar (YYYY o YYYY-MM)
        sort_cols = ["date", "month"] if _is_monthly(sub) else ["date"]
        sub = sub.sort_values(sort_cols)

        s = _safe_numeric_series(sub)
        if s.empty:
            continue

        last_row = _last_numeric_row(sub)
        if last_row is None:
            continue

        p_last = _period_str(last_row)
        v_last = float(pd.to_numeric(last_row["value"], errors="coerce"))
        v_last_rounded = round(v_last, cfg.round_decimals)

        latest_by_indicator[indicator] = (p_last, v_last)

        if _is_monthly(sub):
            td = _trend_direction(s, window=6)
            lines.append(f"- {indicator}: latest {v_last_rounded} ({p_last}); 6M trend={td}")
        else:
            # anual: intentamos un CAGR "span" usando 6 puntos recientes (si existen)
            tmp = sub.copy()
            tmp["_value_num"] = pd.to_numeric(tmp["value"], errors="coerce")
            tmp = tmp.dropna(subset=["_value_num"])

            if len(tmp) >= 6:
                first_row = tmp.iloc[-6]
                y_first = int(first_row["date"])
                y_last = int(last_row["date"])
                span = max(1, y_last - y_first)

                c = _cagr(float(first_row["_value_num"]), float(last_row["_value_num"]), span)
                if c is not None:
                    lines.append(
                        f"- {indicator}: latest {v_last_rounded} ({p_last}); approx CAGR({span}y)={round(c*100,2)}%"
                    )
                else:
                    lines.append(f"- {indicator}: latest {v_last_rounded} ({p_last})")
            else:
                lines.append(f"- {indicator}: latest {v_last_rounded} ({p_last})")

    # ✅ FIX: estos nombres deben coincidir con tus YAML (world/frameworks.yaml)
    top_key = "income_share_held_by_highest_10_percent"
    bot_key = "income_share_held_by_lowest_10_percent"

    if top_key in latest_by_indicator and bot_key in latest_by_indicator:
        _, top_v = latest_by_indicator[top_key]
        _, bot_v = latest_by_indicator[bot_key]
        spread = top_v - bot_v
        lines.append(f"- income_distribution_spread (top10 - bottom10): {round(spread, cfg.round_decimals)} pp")

    return "\n".join(lines)


def build_structured_series_block(df_country: pd.DataFrame, cfg: SeriesConfig) -> str:
    """
    Bloque de series temporales recortadas:
    - Mensual: últimos cfg.max_points_monthly
    - Anual: últimos cfg.max_points_annual
    """
    if df_country is None or df_country.empty:
        return "STRUCTURED SERIES:\n- NO DATA."

    lines = ["STRUCTURED SERIES (truncated):"]

    for indicator in sorted(df_country["indicator"].dropna().unique()):
        sub = df_country[df_country["indicator"] == indicator].copy()
        if sub.empty:
            continue

        monthly = _is_monthly(sub)
        sort_cols = ["date", "month"] if monthly else ["date"]
        sub = sub.sort_values(sort_cols)

        max_points = cfg.max_points_monthly if monthly else cfg.max_points_annual

        # dejamos solo valores numéricos
        sub["_value_num"] = pd.to_numeric(sub["value"], errors="coerce")
        sub = sub.dropna(subset=["_value_num"]).tail(max_points)

        if sub.empty:
            continue

        lines.append(f"\nIndicator: {indicator}")
        for _, row in sub.iterrows():
            period = _period_str(row)
            val = float(row["_value_num"])
            lines.append(f"{period} | {round(val, cfg.round_decimals)}")

    return "\n".join(lines)


def generate_economics_briefing(
    df: pd.DataFrame,
    geo: str,
    base_prompt: str,
    cfg: Optional[SeriesConfig] = None,
) -> str:
    """
    Función principal: construye el prompt final y llama al modelo.

    Args:
        df: DataFrame largo (varios países)
        geo: ISO2 del país
        base_prompt: prompt base del YAML
        cfg: configuración de truncado/decimales

    Returns:
        Texto generado por el modelo.
    """
    cfg = cfg or SeriesConfig()

    df_country = df[df["geo"] == geo].copy()
    if df_country.empty:
        return f"No data available for geo={geo}."

    summary_block = build_summary_block(df_country, cfg)
    series_block = build_structured_series_block(df_country, cfg)

    full_prompt = f"{base_prompt}\n\n{summary_block}\n\n{series_block}\n"
    return generate_text(full_prompt)