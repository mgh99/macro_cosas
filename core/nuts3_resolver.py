# core/nuts3_resolver.py
from __future__ import annotations

import difflib
import json
from pathlib import Path
from typing import Dict, List, Tuple

from connectors.eurostat import eurostat_get

CACHE_PATH = Path("data/cache/nuts3_geo_labels_demo_r_d3area.json")


def _download_geo_labels(dataset: str) -> Dict[str, str]:
    """
    Descarga labels del dimension 'geo' (code -> label) usando un payload pequeño.
    Ojo: solo trae los geos disponibles para ese dataset (perfecto para nosotros).
    """
    js = eurostat_get(dataset, params={"lastTimePeriod": 1}, lang="EN")
    labels = (
        js.get("dimension", {})
          .get("geo", {})
          .get("category", {})
          .get("label", {})
    )
    if not isinstance(labels, dict) or not labels:
        raise ValueError(f"Could not retrieve geo labels for dataset={dataset}")
    return {str(k).strip(): str(v).strip() for k, v in labels.items()}


def _load_cached_geo_labels(dataset: str) -> Dict[str, str]:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

    if CACHE_PATH.exists():
        try:
            obj = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(obj, dict) and obj:
                return {str(k): str(v) for k, v in obj.items()}
        except Exception:
            pass  # si cache corrupta, re-descarga

    labels = _download_geo_labels(dataset)
    CACHE_PATH.write_text(json.dumps(labels, ensure_ascii=False, indent=2), encoding="utf-8")
    return labels


def _best_match(query: str, choices: List[str]) -> Tuple[str | None, float]:
    """
    Returns (best_choice, score 0..1) using difflib ratio.
    """
    if not choices:
        return None, 0.0
    q = (query or "").strip().lower()
    if not q:
        return None, 0.0

    # difflib needs actual strings
    lowered = [c.lower() for c in choices]
    # best by ratio
    best = None
    best_score = 0.0
    for orig, low in zip(choices, lowered):
        score = difflib.SequenceMatcher(None, q, low).ratio()
        if score > best_score:
            best = orig
            best_score = score
    return best, best_score


def resolve_nuts3_inputs(
    inp: str,
    dataset_for_geo_labels: str = "demo_r_d3area",
    min_score: float = 0.70,
    show_suggestions: int = 5,
) -> List[str]:
    """
    Convierte un input del usuario tipo:
      "Madrid, Barcelona, ES300"
    a lista de códigos geo Eurostat NUTS3 (p.ej. ["ES300","ES511"]).

    - Si token ya parece un código (alfa-num) y existe en labels -> se acepta.
    - Si no, fuzzy match contra labels.
    """
    labels = _load_cached_geo_labels(dataset_for_geo_labels)  # code -> label
    codes = set(labels.keys())
    names = list(labels.values())

    tokens = [t.strip() for t in (inp or "").split(",") if t.strip()]
    if not tokens:
        return []

    out: List[str] = []

    for t in tokens:
        t_up = t.upper()

        # 1) si ya es un code válido en este dataset
        if t_up in codes:
            out.append(t_up)
            continue

        # 2) exact match por label (case-insensitive)
        lowered_map = {v.lower(): k for k, v in labels.items()}
        if t.lower() in lowered_map:
            out.append(lowered_map[t.lower()])
            continue

        # 3) fuzzy match
        best, score = _best_match(t, names)
        if best is None or score < min_score:
            # sugerencias top-N (best by ratio)
            scored = []
            for nm in names:
                sc = difflib.SequenceMatcher(None, t.lower(), nm.lower()).ratio()
                scored.append((sc, nm))
            scored.sort(reverse=True, key=lambda x: x[0])
            sug = [f"{nm} ({labels_inv(nm, labels)})" for _, nm in scored[:show_suggestions]]

            raise ValueError(
                f"NUTS3: no match for '{t}'. "
                f"Try a NUTS code (e.g. ES300) or a more specific name.\n"
                f"Suggestions: {', '.join(sug)}"
            )

        code = labels_inv(best, labels)
        print(f"✅ NUTS3 match: '{t}' → '{best}' ({code}) [score={round(score, 2)}]")
        out.append(code)

    # dedupe keep order
    seen = set()
    deduped = []
    for c in out:
        if c not in seen:
            deduped.append(c)
            seen.add(c)
    return deduped


def labels_inv(label: str, labels: Dict[str, str]) -> str:
    """Find code by label (exact) (helper)."""
    for k, v in labels.items():
        if v == label:
            return k
    return ""

def nuts3_code_to_label_map(dataset_for_geo_labels: str = "demo_r_d3area") -> Dict[str, str]:
    """
    Returns dict: { "ES300": "Madrid", ... } (solo los geos que existen en ese dataset).
    """
    return _load_cached_geo_labels(dataset_for_geo_labels)  # code -> label