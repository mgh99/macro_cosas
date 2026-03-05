# core/seasonality.py
import pandas as pd


def calculate_seasonality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input:
        df long with columns:
        geo, date (YYYY), month (optional), value

    Returns:
        geo, year, seasonality_ratio, classification
    """

    df = df.copy()
    df["year"] = df["date"].astype(int)
    
    df = df[df["month"].notna()]
    df["month"] = df["month"].astype(int)
    
    results = []

    for (geo, year), group in df.groupby(["geo", "year"]):
        total = group["value"].sum()
        top3 = group.sort_values("value", ascending=False).head(3)["value"].sum()

        ratio = top3 / total if total else None

        if ratio is None:
            label = None
        elif ratio >= 0.45:
            label = "High seasonality"
        elif ratio >= 0.35:
            label = "Moderate seasonality"
        else:
            label = "Low seasonality"

        results.append({
            "geo": geo,
            "year": year,
            "seasonality_ratio": ratio,
            "classification": label
        })

    return pd.DataFrame(results)