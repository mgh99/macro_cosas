import json
from pathlib import Path

import pandas as pd

from connectors.eurostat import fetch_indicator as fetch_eurostat
from connectors.imf import fetch_indicator as fetch_imf
from connectors.oecd import fetch_indicator as fetch_oecd
from core.config_loader import load_config

CONNECTORS = {
    "eurostat": fetch_eurostat,
    "oecd": fetch_oecd,
    "imf": fetch_imf,
}

# Mapping ISO2 -> ISO3 (OECD usa ISO3)
ISO2_TO_OECD = {
    "ES": "ESP",
    "FR": "FRA",
    "DE": "DEU",
}


def main():
    cfg = load_config()

    defaults = cfg["defaults"]
    geos = cfg["countries"]

    start_year = defaults["start_year"]
    end_year = defaults["end_year"]

    all_outputs = []

    for geo in geos:
        for ind in cfg["indicators"]:

            source = ind.get("source", defaults.get("source", "eurostat"))

            if source not in CONNECTORS:
                raise ValueError(f"Unsupported source: {source}")

            fetch_fn = CONNECTORS[source]

            # ==========================
            # EUROSTAT
            # ==========================
            if source == "eurostat":
                df = fetch_fn(
                    dataset=ind["dataset"],
                    indicator_name=ind["name"],
                    geo=geo,
                    start_year=start_year,
                    end_year=end_year,
                    freq=ind.get("freq", defaults["freq"]),
                    unit=ind.get("unit"),
                    filters=ind.get("filters", {}),
                    geo_level=defaults["geo_level"],
                )

            # ==========================
            # OECD
            # ==========================
            elif source == "oecd":

                if geo not in ISO2_TO_OECD:
                    raise ValueError(f"No OECD ISO3 mapping defined for {geo}")

                oecd_geo = ISO2_TO_OECD[geo]

                query_template = ind.get("query_template")
                if not query_template:
                    raise ValueError(
                        f"OECD indicator '{ind['name']}' requires 'query_template' in YAML"
                    )

                query = query_template.format(oecd_geo=oecd_geo)

                df = fetch_fn(
                    dataset=ind["dataset"],
                    query=query,
                    indicator_name=ind["name"],
                    geo=geo,  # keep ISO2 in final output
                    start_year=start_year,
                    end_year=end_year,
                    geo_level=defaults["geo_level"],
                )

            all_outputs.append(df)

    if not all_outputs:
        raise ValueError("No data was fetched. Check configuration.")

    df_long = pd.concat(all_outputs, ignore_index=True)

    df_wide = df_long.pivot_table(
        index=["geo", "date"],
        columns="indicator",
        values="value",
        aggfunc="first",
    ).reset_index()

    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)

    df_long.to_csv(out_dir / "macro_long.csv", index=False)
    df_wide.to_csv(out_dir / "macro_wide.csv", index=False)

    print("Multi-country export complete.")


if __name__ == "__main__":
    main()
