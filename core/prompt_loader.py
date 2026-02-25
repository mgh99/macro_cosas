from pathlib import Path
from typing import Any, Dict

import yaml


def load_prompts(path: str = "config/prompts.yaml") -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Prompts config not found: {p.resolve()}")

    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)