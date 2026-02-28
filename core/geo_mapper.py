# core/geo_mapper.py
import pycountry

# overrides especiales por si algun dia hicieran falta (por ahora con la librería pycountry va bien)
EUROSTAT2_TO_IMF3 = {
    "ES": "ESP",
    "FR": "FRA",
    "DE": "DEU",
    "IT": "ITA",
    "CH": "CHE",
    "US": "USA",
    "GB": "GBR",
}

IMF3_TO_EUROSTAT2 = {v: k for k, v in EUROSTAT2_TO_IMF3.items()}

def to_wb_geo(geo: str) -> str:
    """
    ISO2 -> ISO3 (World Bank)
    Si ya viene ISO3, lo devuelve.
    """
    g = (geo or "").upper()
    if len(g) == 3 and g.isalpha():
        return g
    if len(g) == 2 and g.isalpha():
        c = pycountry.countries.get(alpha_2=g)
        if c:
            return c.alpha_3

    # fallback: usa tu mapping existente si aplica
    return EUROSTAT2_TO_IMF3.get(g, g)

def to_imf_geo(geo: str) -> str:
    """
    ISO2 -> ISO3 for IMF DataMapper.
    If already ISO3, return as is.
    Fallback: pycountry conversion.
    """
    geo = (geo or "").upper().strip()

    # already ISO3
    if len(geo) == 3 and geo.isalpha():
        return geo

    # known overrides (casos especiales)
    if geo in EUROSTAT2_TO_IMF3:
        return EUROSTAT2_TO_IMF3[geo]

    # generic ISO2 -> ISO3
    if len(geo) == 2 and geo.isalpha():
        c = pycountry.countries.get(alpha_2=geo)
        if c and getattr(c, "alpha_3", None):
            return c.alpha_3

    # last resort: return as-is
    return geo


def to_oecd_geo(geo: str) -> str:
    """
    ISO2 -> ISO3 for OECD.
    (same approach, so world works too)
    """
    return to_imf_geo(geo)


def to_iso2(geo: str) -> str:
    """
    ISO3 -> ISO2
    """
    geo = (geo or "").upper().strip()
    if len(geo) == 2 and geo.isalpha():
        return geo
    if geo in IMF3_TO_EUROSTAT2:
        return IMF3_TO_EUROSTAT2[geo]
    if len(geo) == 3 and geo.isalpha():
        c = pycountry.countries.get(alpha_3=geo)
        if c and getattr(c, "alpha_2", None):
            return c.alpha_2
    return geo