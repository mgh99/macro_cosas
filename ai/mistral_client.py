# ai/mistral_client.py
from __future__ import annotations

import os

from dotenv import load_dotenv
from mistralai import Mistral  # pip install mistralai

load_dotenv()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL")

if not MISTRAL_API_KEY:
    raise ValueError("Missing MISTRAL_API_KEY in .env")

client = Mistral(api_key=MISTRAL_API_KEY)


def generate_text(prompt: str) -> str:
    response = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=0.2  # bajo para tono ejecutivo consistente
    )

    return response.choices[0].message.content.strip()