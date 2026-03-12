# core/country_resolver.py
from __future__ import annotations

import unicodedata
from pathlib import Path
from typing import Dict, Optional

import pycountry
import yaml


# Aggregate geographic codes that are not ISO2 countries but are valid geo inputs
# for specific indicators (those with allow_aggregates: true in the YAML).
AGGREGATE_GEO_CODES = frozenset({"WEOWORLD", "ADVEC", "EU"})


def _is_valid_iso2(code: str) -> bool:
    code = (code or "").strip().upper()
    if len(code) != 2:
        return False
    return pycountry.countries.get(alpha_2=code) is not None


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _normalize_token(token: str) -> str:
    token = (token or "").strip()
    if not token:
        return ""

    token = _strip_accents(token)
    token = token.upper()

    # normalize separators/spaces
    token = token.replace("-", " ")
    token = token.replace("_", " ")
    token = " ".join(token.split())  # collapse multiple spaces

    # also remove dots in abbreviations like U.K.
    token = token.replace(".", "")

    return token


def load_country_aliases(path: str = "config/country_aliases.yaml") -> Dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}

    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Aliases YAML must be a mapping, got: {type(data)}")

    # normalize keys + values
    out: Dict[str, str] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        nk = _normalize_token(k)
        nv = _normalize_token(v)
        out[nk] = nv
    return out


def resolve_country_to_iso2(
    user_input: str,
    aliases: Optional[Dict[str, str]] = None,
) -> str:
    """
    Resolve user input -> ISO2 (ES, FR, DE, ...)

    Priority:
    1) YAML aliases override
    2) Already ISO2 (VALIDATED)
    3) ISO3 -> ISO2 via pycountry
    4) exact name lookup
    5) fuzzy lookup
    """
    tok = _normalize_token(user_input)
    if not tok:
        raise ValueError("Empty country input")

    # 1) aliases override
    if aliases and tok in aliases:
        code = aliases[tok]

        # alias value as ISO2
        if _is_valid_iso2(code):
            return code

        # allow ISO3 in alias values
        if len(code) == 3 and code.isalpha():
            c = pycountry.countries.get(alpha_3=code)
            if c:
                return c.alpha_2

        # allow aggregate geo codes (WEOWORLD, ADVEC, EU, ...)
        if code in AGGREGATE_GEO_CODES:
            return code

        raise ValueError(f"Alias mapped to unsupported code: {user_input} -> {aliases[tok]}")

    # 2) ISO2 (must be real ISO2)
    if _is_valid_iso2(tok):
        return tok

    # 3) ISO3
    if len(tok) == 3 and tok.isalpha():
        c = pycountry.countries.get(alpha_3=tok)
        if c:
            return c.alpha_2

    # 4) exact name (lookup is already flexible)
    try:
        c = pycountry.countries.lookup(tok.title())
        return c.alpha_2
    except LookupError:
        pass

    # 5) fuzzy
    try:
        matches = pycountry.countries.search_fuzzy(tok)
        if matches:
            return matches[0].alpha_2
    except LookupError:
        pass

    raise ValueError(f"Unsupported/unknown country: '{user_input}'")