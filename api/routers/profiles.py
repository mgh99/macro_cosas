from __future__ import annotations

from pathlib import Path
from typing import List

import yaml
from fastapi import APIRouter, HTTPException

from api.schemas import FrameworkInfo, Profile, ProfileInfo

router = APIRouter(prefix="/profiles", tags=["profiles"])

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _load_frameworks(profile: str) -> dict:
    path = _REPO_ROOT / "config" / "profiles" / profile / "frameworks.yaml"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Profile '{profile}' not found")
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@router.get("", response_model=List[ProfileInfo])
def list_profiles():
    """Return all available profiles with their framework summaries."""
    result = []
    for profile in Profile:
        try:
            cfg = _load_frameworks(profile.value)
        except HTTPException:
            continue
        frameworks_raw = cfg.get("frameworks", {}) or {}
        frameworks = [
            FrameworkInfo(
                key=key,
                label=fw.get("label") or fw.get("name") or key,
                indicator_count=len([i for i in fw.get("indicators", []) if i.get("enabled", True)]),
            )
            for key, fw in frameworks_raw.items()
        ]
        result.append(ProfileInfo(profile=profile.value, frameworks=frameworks))
    return result


@router.get("/{profile}/frameworks", response_model=List[FrameworkInfo])
def get_frameworks(profile: Profile):
    """Return available frameworks for a profile."""
    cfg = _load_frameworks(profile.value)
    frameworks_raw = cfg.get("frameworks", {}) or {}
    return [
        FrameworkInfo(
            key=key,
            label=fw.get("label") or fw.get("name") or key,
            indicator_count=len([i for i in fw.get("indicators", []) if i.get("enabled", True)]),
        )
        for key, fw in frameworks_raw.items()
    ]
