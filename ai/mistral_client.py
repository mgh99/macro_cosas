# ai/mistral_client.py
from __future__ import annotations

"""
MISTRAL CLIENT

Este módulo encapsula la llamada al modelo de Mistral.

Responsabilidades:
- Cargar variables de entorno (.env)
- Inicializar el cliente Mistral
- Hacer llamadas robustas al modelo con retries y backoff

Variables de entorno requeridas:

MISTRAL_API_KEY
    API key de Mistral

MISTRAL_MODEL
    Modelo a usar (ej: mistral-large-latest, mistral-small-latest)

Ejemplo .env:

    MISTRAL_API_KEY=xxxxxxxxxxxxxxxx
    MISTRAL_MODEL=mistral-large-latest

Nota para mantenimiento:
- Si quieres cambiar el modelo globalmente, hazlo en .env
- Si quieres cambiar temperatura o retries, edita las constantes abajo
"""

import os
import random
import time

from dotenv import load_dotenv
from mistralai import Mistral

# ------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------

load_dotenv()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")


if not MISTRAL_API_KEY:
    raise ValueError("Missing MISTRAL_API_KEY in .env")


# ------------------------------------------------------------
# Client initialization
# ------------------------------------------------------------

client = Mistral(api_key=MISTRAL_API_KEY)


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

MAX_RETRIES = 4
TEMPERATURE = 0.2


# ------------------------------------------------------------
# Main function
# ------------------------------------------------------------

def generate_text(prompt: str) -> str:
    """
    Envía un prompt al modelo Mistral y devuelve el texto generado.

    Incluye:
    - retries automáticos
    - backoff exponencial
    - jitter aleatorio (evita thundering herd)

    Args:
        prompt: texto completo enviado al modelo

    Returns:
        str: respuesta generada por el modelo
    """

    last_err: Exception | None = None

    # Intentos con backoff exponencial:
    # 0s → ~1s → ~2s → ~4s (+ jitter)
    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.complete(
                model=MISTRAL_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=TEMPERATURE,
            )

            # Protección básica por si la API cambia
            content = response.choices[0].message.content
            if not content:
                raise ValueError("Empty response from Mistral API")

            return content.strip()

        except Exception as e:
            last_err = e

            # Backoff exponencial + jitter
            sleep_s = (2 ** attempt) + random.uniform(0, 0.6)

            # Si no es el último intento, esperamos
            if attempt < MAX_RETRIES - 1:
                time.sleep(sleep_s)

    # Si todos los intentos fallan, propagamos el error
    raise last_err  # type: ignore