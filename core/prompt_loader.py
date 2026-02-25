# core/prompt_loader.py
from __future__ import annotations

from typing import Any, Dict

import yaml


def load_prompts(path: str = "config/prompts.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_prompt_text(prompts_cfg: Dict[str, Any], framework_name: str) -> str:
    """
    Compatible con:
      prompts[framework]["executive_narrative"]["text"]
    y también con tu forma actual:
      prompts[framework]["prompt_executive_narrative"]["text"]
    """
    fw = prompts_cfg.get(framework_name, {})
    if not isinstance(fw, dict):
        return ""

    # prefer new key
    if "executive_narrative" in fw and isinstance(fw["executive_narrative"], dict):
        return str(fw["executive_narrative"].get("text", "")).strip()

    # backward compatible
    if "prompt_executive_narrative" in fw and isinstance(fw["prompt_executive_narrative"], dict):
        return str(fw["prompt_executive_narrative"].get("text", "")).strip()

    return ""