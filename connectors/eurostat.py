# connectors/eurostat.py
from __future__ import annotations

import itertools
import re
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"


def eurostat_get(dataset: str, params: Dict[str, Any], lang: str = "EN", fmt: str = "JSON") -> Dict[str, Any]:
    url = f"{BASE_URL}/{dataset}"
    params = {"format": fmt, "lang": lang, **params}
    r = requests.get(url, params=params, timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise requests.HTTPError(f"{e}\nEurostat says:\n{r.text[:500]}") from e
    return r.json()


def _ordered_category_codes(dim_obj: Dict[str, Any]) -> List[str]:
    idx = dim_obj["category"]["index"]
    return [code for code, _ in sorted(idx.items(), key=lambda kv: kv[1])]


def jsonstat_to_dataframe(js: Dict[str, Any]) -> pd.DataFrame:
    dims = js["id"]
    dim_meta = js["dimension"]

    categories = []
    for d in dims:
        categories.append(_ordered_category_codes(dim_meta[d]))

    rows = list(itertools.product(*categories))
    df = pd.DataFrame(rows, columns=dims)

    values = js.get("value", [])
    if isinstance(values, list):
        df["value"] = values
    elif isinstance(values, dict):
        df["value"] = [None] * len(df)
        for k, v in values.items():
            df.loc[int(k), "value"] = v
    else:
        raise TypeError(f"Unexpected value type: {type(values)}")

    return df

def add_geo_name(df_raw: pd.DataFrame, js: Dict[str, Any]) -> pd.DataFrame:
    labels = (
        js.get("dimension", {})
          .get("geo", {})
          .get("category", {})
          .get("label", {})
    )  # code -> human name
    if "geo" in df_raw.columns and labels:
        df_raw = df_raw.copy()
        df_raw["geo_name"] = df_raw["geo"].map(labels).fillna(df_raw["geo"])
    return df_raw


def describe_dataset(dataset: str, sample_geo: str = "ES", overrides: Optional[Dict[str, Any]] = None) -> None:
    """
    Descarga un payload pequeño y te imprime:
    - nombres de dimensiones
    - para cada dimensión: algunos códigos y sus labels
    """

    # Defaults mínimos (válidos para muchos datasets)
    params: Dict[str, Any] = {
        "geo": sample_geo,
        "lastTimePeriod": 1,
    }

    # Defaults especiales conocidos (para que no rompa lo que ya te funciona)
    if dataset == "tps00010":
        params.update({"freq": "A", "indic_de": "PC_Y0_14"})

    # Overrides opcionales (por si un dataset requiere dims extra)
    if overrides:
        params.update(overrides)

    js = eurostat_get(dataset, params=params, lang="EN")

    dims = js.get("id", [])
    dim_meta = js.get("dimension", {})

    print(f"\n=== DATASET: {dataset} ===")
    print("Dimensions (order):", dims)

    for d in dims:
        if d not in dim_meta:
            continue
        cat = dim_meta[d].get("category", {})
        labels = cat.get("label", {})
        idx = cat.get("index", {})

        codes = [c for c, _ in sorted(idx.items(), key=lambda kv: kv[1])]
        preview = codes[:12]
        print(f"\n- {d} (n={len(codes)}) preview codes:", preview)
        for c in preview[:6]:
            lab = labels.get(c)
            if lab:
                print(f"    {c} -> {lab}")


def _normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


import re


def label_to_code(js: Dict[str, Any], dim: str, label: str) -> str:
    dim_meta = js.get("dimension", {}).get(dim, {})
    cat = dim_meta.get("category", {})
    labels = cat.get("label", {})  # code -> label

    raw = (label or "").strip()

    # ✅ 1) If user already provided a CODE and it's present in labels
    if raw in labels:
        return raw
    if raw.upper() in labels:
        return raw.upper()

    # ✅ 2) CODE fallback (even if labels don't include it)
    # Works great for Eurostat country codes like TOTAL, DE, FR, IT, US, UK/GB, etc.
    # - no spaces
    # - uppercase letters/digits/underscore
    # - short-ish
    if re.fullmatch(r"[A-Z0-9_]{2,15}", raw.upper()):
        return raw.upper()

    # ✅ 3) Otherwise treat as human label
    target = _normalize_text(raw)
    best = None
    for code, lab in labels.items():
        if _normalize_text(lab) == target:
            return code
        if best is None and target in _normalize_text(lab):
            best = code

    if best:
        return best

    if not labels:
        raise ValueError(
            f"Dimension '{dim}' returned no labels in metadata payload. "
            f"For this dataset, use codes in multi_filters. Label provided: '{label}'"
        )

    raise ValueError(f"Label not found in dimension '{dim}': '{label}'")


def normalize_to_long(df_raw: pd.DataFrame, dataset: str, indicator_name: str, geo_level: str, unit_fallback: Optional[str] = None) -> pd.DataFrame:
    time_str = df_raw["time"].astype(str)
    years = pd.to_numeric(time_str.str.extract(r"(\d{4})")[0], errors="coerce")
    months = pd.to_numeric(time_str.str.extract(r"\d{4}-(\d{2})")[0], errors="coerce")

    base_cols = set(["geo", "time", "value", "unit", "freq"])
    extra_dims = [c for c in df_raw.columns if c not in base_cols]

    out = pd.DataFrame(
        {
            "geo": df_raw.get("geo"),
            "geo_level": geo_level,
            "indicator": indicator_name,
            "date": years,
            "month": months,
            "value": pd.to_numeric(df_raw["value"], errors="coerce"),
            "unit": df_raw.get("unit"),
            "source": f"eurostat:{dataset}",
        }
    )

    # conserva dims extra por si acaso (útil para debugging/otros KPIs)
    for c in extra_dims:
        out[c] = df_raw[c]

    if unit_fallback:
        out["unit"] = out["unit"].fillna(unit_fallback) if "unit" in out.columns else unit_fallback

    out = out.dropna(subset=["geo", "date", "value"]).copy()
    out["date"] = out["date"].astype(int)

    # ✅ elegir dim "más variable" como sub_indicator_short SOLO si NO viene ya creado
    if "sub_indicator_short" not in out.columns and extra_dims:
        nun = {c: out[c].nunique(dropna=True) for c in extra_dims}
        best_dim = max(nun, key=nun.get)
        if nun[best_dim] > 1:
            out["sub_indicator_short"] = out[best_dim]

    return out


def fetch_indicator(
    dataset: str,
    geo: str,
    start_year: int,
    end_year: int,
    freq: str,
    geo_level: str,
    indicator_name: str,
    filters: Dict[str, Any],
    multi_filters: Optional[Dict[str, List[str]]] = None,
    unit_fallback: Optional[str] = None,
) -> pd.DataFrame:
    """
    - filters: dict con dimensiones fijas (ej: unit=PC)
    - multi_filters: dict donde cada key es una dimensi?n y su value es lista de LABELS (EN)
      El conector traduce label->code usando metadata del payload.
    """
    # Probe dimensions with a tiny payload so we can drop invalid filter keys.
    # Algunos datasets exigen dims obligatorias (ej: tps00010 exige indic_de), así que hacemos probe robusto.
    probe_candidates: List[Dict[str, Any]] = [
        {"geo": geo, "freq": freq, "lastTimePeriod": 1},  # normal
        {"geo": geo, "lastTimePeriod": 1},                # fallback sin freq
        {"geo": geo, "lastTimePeriod": 1, **(filters or {})},  # fallback con filters del YAML
    ]

    js_probe = None
    last_err: Optional[Exception] = None
    for probe_params in probe_candidates:
        try:
            js_probe = eurostat_get(dataset, params=probe_params, lang="EN")
            break
        except Exception as e:
            last_err = e
            continue

    if js_probe is None:
        # si ni así, devolvemos el error más informativo
        raise last_err  # type: ignore

    dataset_dims = set(js_probe.get("id", []))
    use_freq = "freq" in dataset_dims

    valid_filters = {k: v for k, v in (filters or {}).items() if k in dataset_dims}

    params: Dict[str, Any] = {
        "geo": geo,
        **({"freq": freq} if use_freq else {}),
        "sinceTimePeriod": start_year,
        "untilTimePeriod": end_year,
        **valid_filters,
    }

    # Si no hay multi, fetch directo
    if not multi_filters:
        js = eurostat_get(dataset, params=params, lang="EN")
        df_raw = jsonstat_to_dataframe(js)
        df_raw = add_geo_name(df_raw, js)

        # Detecta la dimensión "variable" y traduce a label (AFR -> Africa)
        base_cols = {"geo", "time", "value", "unit", "freq"}
        extra_dims = [c for c in df_raw.columns if c not in base_cols]

        if extra_dims:
            nun = {c: df_raw[c].nunique(dropna=True) for c in extra_dims}
            best_dim = max(nun, key=nun.get)

            if nun[best_dim] > 1:
                label_map = (
                    js.get("dimension", {})
                        .get(best_dim, {})
                        .get("category", {})
                        .get("label", {})
                )  # code -> label

                if label_map:
                    df_raw["sub_indicator_short"] = df_raw[best_dim].map(label_map).fillna(df_raw[best_dim])
                else:
                    df_raw["sub_indicator_short"] = df_raw[best_dim]

        return normalize_to_long(df_raw, dataset, indicator_name, geo_level, unit_fallback=unit_fallback)

    resolved_multi_filters: Dict[str, List[str]] = {}
    for dim, label_list in multi_filters.items():
        if dim in dataset_dims:
            resolved_multi_filters[dim] = label_list
            continue

        candidate_dims = [
            d
            for d in js_probe.get("id", [])
            if d not in {"geo", "freq", "time"} and d not in valid_filters
        ]
        if len(candidate_dims) == 1:
            resolved_multi_filters[candidate_dims[0]] = label_list
            continue

        raise ValueError(
            f"Unknown multi_filters dimension '{dim}' for dataset '{dataset}'. Available dimensions: {sorted(dataset_dims)}"
        )

    # Metadata call for label->code mapping must not include since/untilTimePeriod.
    meta_params = {
        k: v
        for k, v in params.items()
        if k not in {"TIME", "TIME_PERIOD", "time", "time_period", "sinceTimePeriod", "untilTimePeriod", "lastTimePeriod"}
    }
    js_meta = eurostat_get(dataset, params={**meta_params, "lastTimePeriod": 1}, lang="EN")

    out_parts: List[pd.DataFrame] = []

    for dim, label_list in resolved_multi_filters.items():
        for lab in label_list:
            code = label_to_code(js_meta, dim, lab)

            js = eurostat_get(dataset, params={**params, dim: code}, lang="EN")

            labels = (
                js_meta.get("dimension", {})
                .get(dim, {})
                .get("category", {})
                .get("label", {})
            )
            human_label = labels.get(code, lab)

            df_raw = jsonstat_to_dataframe(js)

            df_norm = normalize_to_long(df_raw, dataset, indicator_name, geo_level, unit_fallback=unit_fallback)

            def _short_age(code: str) -> str:
                if code == "PC_Y80_MAX":
                    return "80+"
                # PC_Y0_14 -> 0-14, PC_Y15_24 -> 15-24
                if code.startswith("PC_Y"):
                    x = code.replace("PC_Y", "")
                    return x.replace("_", "-")
                return code

            df_norm["sub_indicator_short"] = human_label

            out_parts.append(df_norm)

    if not out_parts:
        return pd.DataFrame(columns=["geo", "geo_level", "indicator", "date", "value", "unit", "source"])

    return pd.concat(out_parts, ignore_index=True)
