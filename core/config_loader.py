# core/config_loader.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


def load_config(path: str = "config/frameworks.yaml") -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p.resolve()}")

    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)