# core/top_origins.py
import pandas as pd


def top_origins(df_1b: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    df = df_1b.copy()

    last_year = df.groupby("geo")["date"].transform("max")
    df = df[df["date"] == last_year]

    exclude = {"TOTAL"}
    df = df[~df["sub_indicator_short"].isin(exclude)]

    df = df.sort_values(["geo", "value"], ascending=[True, False])
    df["rank"] = df.groupby("geo").cumcount() + 1

    return df[df["rank"] <= top_n][["geo", "date", "sub_indicator_short", "value", "rank"]]