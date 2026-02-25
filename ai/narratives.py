# ai/narratives.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from ai.commentary import chat_complete
from core.prompt_loader import get_prompt_text, load_prompts


def _data_snapshot_for_framework(df_long: pd.DataFrame, framework_cfg: Dict[str, Any]) -> str:
    """
    Convierte los datos disponibles en un resumen compacto para meterlo al prompt.
    No inventa nada: solo lista series por indicador y valores año=valor.
    """
    # framework_cfg["indicators"] contiene los nombres
    indicator_names = [i.get("name") for i in framework_cfg.get("indicators", []) if i.get("name")]
    df = df_long[df_long["indicator"].isin(indicator_names)].copy()
    if df.empty:
        return "No data available for this framework."

    lines = []
    for (geo, ind), g in df.groupby(["geo", "indicator"]):
        g = g.sort_values("date")
        pairs = ", ".join(f"{int(r.date)}={float(r.value):.2f}" for r in g.itertuples())
        lines.append(f"- geo={geo} | indicator={ind} | series: {pairs}")
    return "\n".join(lines)


def generate_narratives(
    framework_cfg: Dict[str, Any],
    prompts_path: str = "config/prompts.yaml",
    input_long_csv: str = "outputs/macro_long.csv",
    output_md: str = "outputs/narratives.md",
) -> None:
    prompts_cfg = load_prompts(prompts_path)
    df_long = pd.read_csv(input_long_csv)

    required = {"geo", "indicator", "date", "value"}
    if not required.issubset(set(df_long.columns)):
        raise ValueError(f"{input_long_csv} missing required columns: {required - set(df_long.columns)}")

    frameworks = framework_cfg.get("frameworks", {})
    if not isinstance(frameworks, dict) or not frameworks:
        raise ValueError("framework.yaml has no 'frameworks' section.")

    out_lines = ["# Executive Narratives\n"]

    for fw_name, fw in frameworks.items():
        prompt_text = get_prompt_text(prompts_cfg, fw_name)
        out_lines.append(f"## {fw_name}\n")

        if not prompt_text:
            out_lines.append("_No prompt found for this framework._\n")
            continue

        snapshot = _data_snapshot_for_framework(df_long, fw)

        messages = [
            {"role": "system", "content": "Use ONLY the provided data. Do not invent data. If data is missing, state it clearly."},
            {"role": "user", "content": f"{prompt_text}\n\nSTRUCTURED DATA (ONLY SOURCE OF TRUTH):\n{snapshot}"},
        ]

        try:
            narrative = chat_complete(messages)
        except Exception as e:
            narrative = f"[AI_ERROR] {type(e).__name__}: {e}"

        out_lines.append(narrative.strip() + "\n")

    out_path = Path(output_md)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out_lines), encoding="utf-8")
    print(f"✅ Narratives written to {out_path}")