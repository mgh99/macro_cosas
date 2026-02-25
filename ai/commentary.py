# ai/commentary.py
import os
from typing import Dict, List

from dotenv import load_dotenv
from mistralai import Mistral

load_dotenv()


def chat_complete(messages: List[Dict[str, str]]) -> str:
    api_key = os.getenv("MISTRAL_API_KEY")
    model = os.getenv("MISTRAL_MODEL", "mistral-small-2506")

    if not api_key:
        raise ValueError("MISTRAL_API_KEY is missing. Put it in .env or environment variables.")

    with Mistral(api_key=api_key) as mistral:
        res = mistral.chat.complete(
            model=model,
            messages=messages,
            stream=False,
        )

    return res.choices[0].message.content.strip()