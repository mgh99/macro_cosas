# ai/economics_analyzer.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd

from ai.mistral_client import generate_text


@dataclass
class SeriesConfig:
    # Cuántos puntos máximos por indicador (para no reventar el prompt)
    max_points_monthly: int = 24   # últimos 24 meses
    max_points_annual: int = 12    # últimos 12 años
    # redondeo
    round_decimals: int = 3


def _period_str(row: pd.Series) -> str:
    y = int(row["date"])
    m = row.get("month", pd.NA)
    if pd.notna(m):
        return f"{y}-{int(m):02d}"
    return str(y)


def _safe_numeric_series(df_ind: pd.DataFrame) -> pd.Series:
    s = pd.to_numeric(df_ind["value"], errors="coerce")
    return s.dropna()


def _cagr(first: float, last: float, years: int) -> Optional[float]:
    if years <= 0:
        return None
    if first <= 0 or last <= 0:
        return None
    return (last / first) ** (1 / years) - 1


def _trend_direction(s: pd.Series, window: int = 6) -> str:
    # tendencia simple: compara media de últimas N vs N anteriores
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


def build_summary_block(df_country: pd.DataFrame, cfg: SeriesConfig) -> str:
    """
    Produce un bloque compacto con métricas útiles:
    - latest value (y periodo)
    - trend (para series mensuales)
    - CAGR (si aplica y hay años suficientes)
    - spread top10-bottom10 si están ambos
    """
    lines = ["SUMMARY METRICS (computed only from provided data):"]

    # helpers
    latest_by_indicator: Dict[str, Tuple[str, float]] = {}

    for indicator in sorted(df_country["indicator"].dropna().unique()):
        sub = df_country[df_country["indicator"] == indicator].copy()
        if sub.empty:
            continue

        sort_cols = ["date", "month"] if "month" in sub.columns else ["date"]
        sub = sub.sort_values(sort_cols)

        s = _safe_numeric_series(sub)
        if s.empty:
            continue

        last_row = sub.dropna(subset=["value"]).iloc[-1]
        p_last = _period_str(last_row)
        v_last = float(last_row["value"])
        v_last_rounded = round(v_last, cfg.round_decimals)

        latest_by_indicator[indicator] = (p_last, v_last)

        # trend only meaningful for monthly
        if "month" in sub.columns and sub["month"].notna().any():
            td = _trend_direction(s, window=6)
            lines.append(f"- {indicator}: latest {v_last_rounded} ({p_last}); 6M trend={td}")
        else:
            # annual: try CAGR on last 5y / 10y depending on data
            years = sub["date"].dropna().astype(int).tolist()
            if len(years) >= 6:
                # last 5-year span within available points
                first_row = sub.dropna(subset=["value"]).iloc[-6]
                y_first = int(first_row["date"])
                y_last = int(last_row["date"])
                span = max(1, y_last - y_first)
                c = _cagr(float(first_row["value"]), float(last_row["value"]), span)
                if c is not None:
                    lines.append(f"- {indicator}: latest {v_last_rounded} ({p_last}); approx CAGR({span}y)={round(c*100,2)}%")
                else:
                    lines.append(f"- {indicator}: latest {v_last_rounded} ({p_last})")
            else:
                lines.append(f"- {indicator}: latest {v_last_rounded} ({p_last})")

    # inequality spread if present
    top_key = "income_share_held_by_highest_10%"
    bot_key = "income_share_held_by_lowest_10%"
    if top_key in latest_by_indicator and bot_key in latest_by_indicator:
        _, top_v = latest_by_indicator[top_key]
        _, bot_v = latest_by_indicator[bot_key]
        spread = top_v - bot_v
        lines.append(f"- income_distribution_spread (top10 - bottom10): {round(spread, cfg.round_decimals)} pp")

    return "\n".join(lines)


def build_structured_series_block(df_country: pd.DataFrame, cfg: SeriesConfig) -> str:
    """
    Bloque de series "recortadas": últimas N observaciones por indicador,
    para que el modelo tenga contexto temporal sin explotar tokens.
    """
    lines = ["STRUCTURED SERIES (truncated):"]

    for indicator in sorted(df_country["indicator"].dropna().unique()):
        sub = df_country[df_country["indicator"] == indicator].copy()
        if sub.empty:
            continue

        sort_cols = ["date", "month"] if "month" in sub.columns else ["date"]
        sub = sub.sort_values(sort_cols)

        # truncate
        is_monthly = "month" in sub.columns and sub["month"].notna().any()
        max_points = cfg.max_points_monthly if is_monthly else cfg.max_points_annual

        sub = sub.dropna(subset=["value"]).tail(max_points)

        lines.append(f"\nIndicator: {indicator}")
        for _, row in sub.iterrows():
            period = _period_str(row)
            val = row["value"]
            if pd.isna(val):
                continue
            lines.append(f"{period} | {round(float(val), cfg.round_decimals)}")

    return "\n".join(lines)


def generate_economics_briefing(
    df: pd.DataFrame,
    geo: str,
    base_prompt: str,
    cfg: Optional[SeriesConfig] = None,
) -> str:
    cfg = cfg or SeriesConfig()

    df_country = df[df["geo"] == geo].copy()
    if df_country.empty:
        return f"No data available for geo={geo}."

    summary_block = build_summary_block(df_country, cfg)
    series_block = build_structured_series_block(df_country, cfg)

    full_prompt = (
        f"{base_prompt}\n\n"
        f"{summary_block}\n\n"
        f"{series_block}\n"
    )

    return generate_text(full_prompt)