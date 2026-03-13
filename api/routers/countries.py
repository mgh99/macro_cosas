from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from fastapi import APIRouter

from api.schemas import CountryResolveRequest, CountryResolveResponse

router = APIRouter(prefix="/resolve-countries", tags=["countries"])

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@router.post("", response_model=CountryResolveResponse)
def resolve_countries(req: CountryResolveRequest):
    """
    Resolve a list of country strings to ISO2 codes using the project's
    country resolver (supports fuzzy matching, Spanish names, ISO3, etc).
    """
    import sys
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))

    from core.country_resolver import load_country_aliases, resolve_country_to_iso2

    aliases_path = str(_REPO_ROOT / "config" / "profiles" / req.profile.value / "country_aliases.yaml")
    aliases = load_country_aliases(aliases_path)

    resolved: Dict[str, Optional[str]] = {}
    errors: Dict[str, str] = {}

    for country in req.countries:
        try:
            resolved[country] = resolve_country_to_iso2(country, aliases)
        except ValueError as e:
            resolved[country] = None
            errors[country] = str(e)

    return CountryResolveResponse(resolved=resolved, errors=errors)
