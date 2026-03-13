from __future__ import annotations

import os
from typing import Optional

# In-memory settings (survives the process lifetime; persisted via os.environ)
_mistral_key: Optional[str] = None


def set_mistral_key(key: str) -> None:
    global _mistral_key
    _mistral_key = key.strip()
    os.environ["MISTRAL_API_KEY"] = _mistral_key


def get_mistral_key() -> Optional[str]:
    # Priority: in-memory setting > env var already loaded
    return _mistral_key or os.environ.get("MISTRAL_API_KEY") or None


def mistral_key_is_set() -> bool:
    return bool(get_mistral_key())


def clear_mistral_key() -> None:
    global _mistral_key
    _mistral_key = None
    os.environ.pop("MISTRAL_API_KEY", None)
