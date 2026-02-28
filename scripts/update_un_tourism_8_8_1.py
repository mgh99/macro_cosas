# scripts/update_un_tourism_8_8_1.py
from __future__ import annotations

import zipfile
from pathlib import Path

import requests

ZIP_URL = "https://pre-webunwto.s3.amazonaws.com/s3fs-public/2025-07/UN_Tourism_8_9_1_TDGDP_04_2025.zip"

OUT_DIR = Path("data/un_tourism/8_9_1")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ZIP_PATH = OUT_DIR / "source.zip"

UA = "macro-cosas/1.0"


def download_zip():
    print("Downloading UN Tourism 8.9.1 dataset...")
    r = requests.get(ZIP_URL, timeout=120, headers={"User-Agent": UA})
    r.raise_for_status()
    ZIP_PATH.write_bytes(r.content)
    print(f"Saved ZIP to {ZIP_PATH}")


def extract_zip():
    print("Extracting ZIP...")
    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        z.extractall(OUT_DIR)
    print("Extraction complete.")


def rename_excel():
    # Busca el primer .xlsx dentro del directorio
    for p in OUT_DIR.glob("*.xlsx"):
        target = OUT_DIR / "dataset.xlsx"
        p.replace(target)
        print(f"Renamed {p.name} -> dataset.xlsx")
        return
    print("No Excel file found.")


def main():
    download_zip()
    extract_zip()
    rename_excel()
    print("✅ UN Tourism 8.9.1 dataset ready.")


if __name__ == "__main__":
    main()