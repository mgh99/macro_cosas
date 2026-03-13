from __future__ import annotations

from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException

from api.settings import clear_mistral_key, mistral_key_is_set, set_mistral_key

router = APIRouter(prefix="/settings", tags=["settings"])


class MistralKeyRequest(BaseModel):
    api_key: str = Field(..., min_length=1)


@router.get("")
def get_settings():
    """Return current settings status (never exposes the actual key)."""
    return {"mistral_key_set": mistral_key_is_set()}


@router.post("/mistral-key")
def save_mistral_key(req: MistralKeyRequest):
    """Store the Mistral API key for the current server session."""
    if not req.api_key.strip():
        raise HTTPException(status_code=422, detail="API key cannot be empty")
    set_mistral_key(req.api_key)
    return {"ok": True, "mistral_key_set": True}


@router.delete("/mistral-key")
def remove_mistral_key():
    """Clear the stored Mistral API key."""
    clear_mistral_key()
    return {"ok": True, "mistral_key_set": False}
