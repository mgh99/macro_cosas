# core/geo_mapper.py

EUROSTAT2_TO_IMF3 = {
    "ES": "ESP",
    "FR": "FRA",
    "DE": "DEU",
    "IT": "ITA",
    "CH": "CHE",
    "US": "USA",
    "GB": "GBR",
}

# invertimos el mapping automáticamente
IMF3_TO_EUROSTAT2 = {v: k for k, v in EUROSTAT2_TO_IMF3.items()}


def to_imf_geo(geo: str) -> str:
    """
    ISO2 -> ISO3 (IMF / OECD format)
    If already ISO3, return as is.
    """
    geo = (geo or "").upper()
    return EUROSTAT2_TO_IMF3.get(geo, geo)


def to_oecd_geo(geo: str) -> str:
    """
    ISO2 -> ISO3 (OECD format)
    """
    geo = (geo or "").upper()
    return EUROSTAT2_TO_IMF3.get(geo, geo)


def to_iso2(geo: str) -> str:
    """
    ISO3 -> ISO2
    If already ISO2, return as is.
    """
    geo = (geo or "").upper()
    return IMF3_TO_EUROSTAT2.get(geo, geo)