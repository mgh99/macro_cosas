# core/geo_to_m49.py
from __future__ import annotations

import pycountry


def iso2_to_m49(iso2: str) -> int:
    iso2 = (iso2 or "").strip().upper()
    c = pycountry.countries.get(alpha_2=iso2)
    if not c or not getattr(c, "numeric", None):
        raise ValueError(f"Cannot map ISO2 to M49 numeric: {iso2}")
    return int(c.numeric)