# ai/mistral_client.py
from __future__ import annotations

import os
import random
import time

from dotenv import load_dotenv
from mistralai import Mistral

load_dotenv()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL")

if not MISTRAL_API_KEY:
    raise ValueError("Missing MISTRAL_API_KEY in .env")

client = Mistral(api_key=MISTRAL_API_KEY)


def generate_text(prompt: str) -> str:
    """
    Robust call with retries for transient 5xx / service unavailable.
    """
    last_err: Exception | None = None

    # 4 intentos: 0s, ~1s, ~2s, ~4s (+ jitter)
    for attempt in range(4):
        try:
            response = client.chat.complete(
                model=MISTRAL_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            return response.choices[0].message.content.strip()

        except Exception as e:
            last_err = e

            # Backoff exponencial + jitter
            sleep_s = (2**attempt) + random.uniform(0, 0.6)
            # Si es el último intento, no duermas, lanza error
            if attempt < 3:
                time.sleep(sleep_s)

    raise last_err  # type: ignore