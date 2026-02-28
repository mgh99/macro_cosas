from connectors.oecd import fetch_indicator

COMMON = dict(
    dataset_id="OECD.SDD.STES,DSD_STES@DF_CLI,",
    geos_iso3=["ESP","FRA","DEU"],
    start_period="2019-01",
    end_period="2024-12",
    indicator_name="consumer_confidence_index",
    unit_fallback="index",
)

# Probe A (lo que tú crees: consumer confidence)
df_a = fetch_indicator(selection_template="{geo}.M.CCICP...AA...H", **COMMON)
print("A rows:", len(df_a), "geos:", df_a["geo"].unique()[:10])

# Probe B (lo que te da la URL)
df_b = fetch_indicator(selection_template="{geo}.M.LI...AA...H", **COMMON)
print("B rows:", len(df_b), "geos:", df_b["geo"].unique()[:10])