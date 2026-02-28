# core/cache_manager.py
from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path
from typing import Optional

import requests


def _safe_name_from_url(url: str) -> str:
    # súper simple y suficiente para cache
    name = url.split("/")[-1].split("?")[0]
    return "".join(c if c.isalnum() or c in {"-", "_", "."} else "_" for c in name)


def download_file(url: str, out_path: Path, timeout: int = 120) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path

    headers = {
        # user-agent de navegador
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
        "Connection": "keep-alive",
        # IMPORTANTÍSIMO a veces: referer/origin del site
        "Referer": "https://www.untourism.int/",
        "Origin": "https://www.untourism.int",
    }

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return out_path


def ensure_zip_extracted(
    target_dir: Path,
    zip_path: Optional[Path] = None,
    zip_url: Optional[str] = None,
) -> Path:
    """
    Extrae un ZIP en target_dir con cache.

    - Si target_dir ya tiene archivos -> no hace nada.
    - Si zip_path existe -> lo extrae.
    - Si no, si zip_url -> lo descarga a target_dir.parent y lo extrae.
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    # cache hit
    if any(target_dir.iterdir()):
        return target_dir

    if zip_path is not None:
        zip_path = Path(zip_path)
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP not found: {zip_path}")

        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(target_dir)
        return target_dir

    if zip_url:
        zip_name = _safe_name_from_url(zip_url)
        if not zip_name.lower().endswith(".zip"):
            zip_name += ".zip"
        local_zip = target_dir.parent / zip_name

        if not local_zip.exists():
            download_file(zip_url, local_zip)

        with zipfile.ZipFile(local_zip, "r") as z:
            z.extractall(target_dir)
        return target_dir

    raise ValueError("ensure_zip_extracted requires zip_path or zip_url")