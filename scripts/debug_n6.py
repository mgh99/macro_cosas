from connectors.eurostat import fetch_indicator

df = fetch_indicator(
    dataset="tour_occ_nin3",
    geo="ES112",
    start_year=2020,
    end_year=2024,
    freq="A",
    geo_level="city",
    indicator_name="N6_test",
    filters={"unit": "NR", "nace_r2": "I551"},
    multi_filters={"c_resid": ["DOM", "FOR", "TOTAL"]},
)

print(df.head(20))
print("rows:", len(df))
print("unique sub_indicator_short:", df["sub_indicator_short"].unique() if "sub_indicator_short" in df.columns else None)