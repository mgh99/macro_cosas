from connectors.oecd import fetch_indicator

df = fetch_indicator(
    dataset_id="OECD.SDD.STES,DSD_STES@DF_CLI,",
    selection_template="{geo}.M.CCICP...AA...H",
    geos_iso3=["ESP","FRA","DEU"],
    start_period="2019-01",
    end_period="2024-12",
    indicator_name="consumer_confidence_index",
    unit_fallback="index"
)
print(df.head())
print(df["geo"].unique())