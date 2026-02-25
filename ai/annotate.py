# ai/annotate.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from ai.commentary import chat_complete

INDICATOR_LABELS: Dict[str, str] = {
    "gdp_real_growth_yoy": "Real GDP growth (YoY, %)",
    "hicp_inflation_yoy": "Inflation (HICP, YoY, %)",
    "unemployment_rate": "Unemployment rate (% of labour force)",
    # add more later
}

# Simple “what does it mean” hints (keeps the model grounded and consistent)
INDICATOR_HINTS: Dict[str, str] = {
    "gdp_real_growth_yoy": "Higher values mean faster economic growth; negative values indicate contraction.",
    "hicp_inflation_yoy": "Higher values mean faster price increases; very high inflation can erode purchasing power.",
    "unemployment_rate": "Higher values mean more unemployment; sustained decreases suggest improving labour market.",
}


@dataclass(frozen=True)
class PeriodStats:
    start_year: int
    end_year: int
    start_value: float | None
    end_value: float | None
    min_value: float | None
    max_value: float | None
    avg_value: float | None
    last_delta: float | None  # end - previous year
    trend: str  # up/down/mixed/flat


def compute_period_stats(g: pd.DataFrame, start_year: int, end_year: int) -> PeriodStats:
    gg = g.sort_values("date").copy()
    gg = gg[(gg["date"] >= start_year) & (gg["date"] <= end_year)]

    if gg.empty:
        return PeriodStats(start_year, end_year, None, None, None, None, None, None, "flat")

    vals = gg["value"].astype(float).tolist()
    years = gg["date"].astype(int).tolist()

    start_val = float(vals[0])
    end_val = float(vals[-1])
    min_val = float(min(vals))
    max_val = float(max(vals))
    avg_val = float(sum(vals) / len(vals))

    # last delta: end - previous
    last_delta = None
    if len(vals) >= 2:
        last_delta = float(vals[-1] - vals[-2])

    # Trend heuristic based on start->end and volatility
    change = end_val - start_val
    amplitude = max_val - min_val

    # thresholds (tweakable)
    eps = 0.15  # ~0.15 pp “flat”
    if abs(change) <= eps and amplitude <= 2.0 * eps:
        trend = "flat"
    else:
        # If it mostly moves one direction
        ups = sum(1 for i in range(1, len(vals)) if vals[i] > vals[i - 1])
        downs = sum(1 for i in range(1, len(vals)) if vals[i] < vals[i - 1])
        if ups >= max(2, 2 * downs):
            trend = "up"
        elif downs >= max(2, 2 * ups):
            trend = "down"
        else:
            trend = "mixed"

    return PeriodStats(
        start_year=start_year,
        end_year=end_year,
        start_value=start_val,
        end_value=end_val,
        min_value=min_val,
        max_value=max_val,
        avg_value=avg_val,
        last_delta=last_delta,
        trend=trend,
    )


def build_prompt(geo: str, indicator: str, stats: PeriodStats, series: pd.DataFrame) -> str:
    label = INDICATOR_LABELS.get(indicator, indicator)
    hint = INDICATOR_HINTS.get(indicator, "")

    # Give the model the actual time series (small) to avoid hallucination
    pairs = ", ".join(f"{int(r.date)}={float(r.value):.2f}" for r in series.sort_values("date").itertuples())

    trend_word = {
        "up": "increased overall",
        "down": "decreased overall",
        "mixed": "was volatile / mixed",
        "flat": "was broadly stable",
    }[stats.trend]

    return f"""
You are helping write concise business-development macro commentary.
Country: {geo}
Indicator: {label}
Period: {stats.start_year}-{stats.end_year}

Time series (year=value): {pairs}

Computed summary:
- Start value: {stats.start_value:.2f}
- End value: {stats.end_value:.2f}
- Min/Max: {stats.min_value:.2f}/{stats.max_value:.2f}
- Average: {stats.avg_value:.2f}
- Latest YoY change vs previous year: {stats.last_delta:.2f if stats.last_delta is not None else "NA"}
- Overall pattern: {trend_word}

Guidance: {hint}

Task:
Write 1-2 short sentences in Spanish describing the trend and what it implies (no forecasts, no politics, no made-up causes).
Be specific: refer to the direction and any notable peaks/troughs if visible.
""".strip()


def annotate_csv(
    input_csv: str = "outputs/macro_long.csv",
    output_csv: str = "outputs/macro_long_annotated.csv",
) -> None:
    df = pd.read_csv(input_csv)

    required = {"geo", "indicator", "date", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV missing columns: {missing}")

    # Load defaults from the data itself (or set your own)
    start_year = int(df["date"].min())
    end_year = int(df["date"].max())

    # Per-row delta & direction
    df = df.sort_values(["geo", "indicator", "date"]).copy()
    df["delta_pp"] = df.groupby(["geo", "indicator"])["value"].diff()
    df["direction"] = df["delta_pp"].apply(
        lambda x: "flat" if pd.isna(x) or abs(x) < 0.15 else ("up" if x > 0 else "down")
    )

    # Generate one IA commentary per (geo, indicator)
    commentary_map: Dict[Tuple[str, str], str] = {}

    for (geo, indicator), g in df.groupby(["geo", "indicator"], sort=False):
        g_period = g[(g["date"] >= start_year) & (g["date"] <= end_year)][["date", "value"]].dropna()
        if g_period.empty:
            commentary_map[(geo, indicator)] = ""
            continue

        stats = compute_period_stats(g_period, start_year, end_year)
        prompt = build_prompt(geo, indicator, stats, g_period)

        messages = [
            {"role": "system", "content": "You produce factual commentary only from provided data."},
            {"role": "user", "content": prompt},
        ]

        try:
            text = chat_complete(messages)
        except Exception as e:
            text = f"[AI_ERROR] {type(e).__name__}: {e}"

        commentary_map[(geo, indicator)] = text

    df["period_commentary"] = df.apply(lambda r: commentary_map.get((r["geo"], r["indicator"]), ""), axis=1)

    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"✅ Annotated CSV written to {out_path}")


if __name__ == "__main__":
    annotate_csv()