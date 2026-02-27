from __future__ import annotations

import unicodedata
from pathlib import Path
from typing import Dict, Optional

import pycountry
import yaml


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
        # value should be ISO2 ideally
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
    2) Already ISO2
    3) ISO3 -> ISO2 via pycountry
    4) exact name lookup
    5) fuzzy lookup
    """
    tok = _normalize_token(user_input)
    if not tok:
        raise ValueError("Empty country input")

    # 1) aliases override
    if aliases and tok in aliases:
        iso2 = aliases[tok]
        if len(iso2) == 2 and iso2.isalpha():
            return iso2
        # allow iso3 in alias values
        if len(iso2) == 3 and iso2.isalpha():
            c = pycountry.countries.get(alpha_3=iso2)
            if c:
                return c.alpha_2
        raise ValueError(f"Alias mapped to unsupported code: {user_input} -> {aliases[tok]}")

    # 2) ISO2
    if len(tok) == 2 and tok.isalpha():
        return tok

    # 3) ISO3
    if len(tok) == 3 and tok.isalpha():
        c = pycountry.countries.get(alpha_3=tok)
        if c:
            return c.alpha_2

    # 4) exact name (try common variants)
    # pycountry expects title-case names
    try:
        c = pycountry.countries.lookup(tok.title())
        # lookup() is flexible, but can throw LookupError
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