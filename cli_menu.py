# cli_menu.py
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

from core.config_loader import load_config
from core.country_resolver import load_country_aliases, resolve_country_to_iso2


def choose_profile(profiles: Dict) -> str:
    keys = list(profiles.keys())
    print("\n🧭 Choose profile:")
    for i, k in enumerate(keys, start=1):
        print(f"  {i}) {k} — {profiles[k].get('title','')}")
    raw = input("Select option (1..n): ").strip()
    if not raw or not raw.isdigit():
        return keys[0]
    idx = int(raw)
    if 1 <= idx <= len(keys):
        return keys[idx - 1]
    return keys[0]

def ask_yes_no(prompt: str, default: bool = True) -> bool:
    suffix = " [Y/n] " if default else " [y/N] "
    while True:
        s = input(prompt + suffix).strip().lower()
        if not s:
            return default
        if s in {"y", "yes", "si", "sí"}:
            return True
        if s in {"n", "no"}:
            return False
        print("Please type y/n.")


def choose_output_dir() -> Path:
    print("\n📁 Output folder:")
    print("  1) Default (./outputs)")
    print("  2) Desktop")
    print("  3) Custom path")
    choice = input("Select option (1/2/3): ").strip() or "1"

    if choice == "1":
        p = Path("outputs")
    elif choice == "2":
        p = Path.home() / "Desktop" / "macro_outputs"
    elif choice == "3":
        raw = input("Enter full path: ").strip()
        if not raw:
            raise ValueError("Custom path cannot be empty.")
        p = Path(raw)
    else:
        print("Invalid option, using default ./outputs")
        p = Path("outputs")

    p.mkdir(parents=True, exist_ok=True)
    return p


def choose_frameworks(frameworks: Dict) -> List[str]:
    keys = list(frameworks.keys())
    if not keys:
        raise ValueError("No frameworks found in selected profile frameworks file.")

    print("\n🧱 Available frameworks:")
    for i, k in enumerate(keys, start=1):
        print(f"  {i}) {k}")
    print(f"  {len(keys) + 1}) ALL")

    raw = input("Select option (e.g. 1,3 or ALL): ").strip().upper()
    if not raw:
        return keys

    if raw in {"ALL", str(len(keys) + 1)}:
        return keys

    picks = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit():
            raise ValueError(f"Invalid selection token: '{part}'")
        idx = int(part)
        if idx < 1 or idx > len(keys):
            raise ValueError(f"Selection out of range: {idx}")
        picks.append(keys[idx - 1])

    seen = set()
    out = []
    for x in picks:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def choose_outputs() -> Dict[str, bool]:
    print("\n📦 Outputs:")
    print("  1) Full package (CSV + Excel by indicator + single-sheet + AI*)")
    print("  2) CSV only")
    print("  3) Excel by indicator only")
    print("  4) Single-sheet Excel only")
    print("  5) No files (debug run)")

    choice = input("Select option (1/2/3/4/5): ").strip() or "1"

    if choice == "1":
        return {"csv": True, "excel_by_indicator": True, "single_sheet": True, "debug_no_files": False}
    if choice == "2":
        return {"csv": True, "excel_by_indicator": False, "single_sheet": False, "debug_no_files": False}
    if choice == "3":
        return {"csv": False, "excel_by_indicator": True, "single_sheet": False, "debug_no_files": False}
    if choice == "4":
        return {"csv": False, "excel_by_indicator": False, "single_sheet": True, "debug_no_files": False}
    if choice == "5":
        return {"csv": False, "excel_by_indicator": False, "single_sheet": False, "debug_no_files": True}

    print("Invalid option, using Full package.")
    return {"csv": True, "excel_by_indicator": True, "single_sheet": True, "debug_no_files": False}


def parse_countries_with_aliases(inp: str, aliases: Dict[str, str]) -> List[str]:
    tokens = [t.strip() for t in inp.split(",") if t.strip()]
    if not tokens:
        raise ValueError("No countries provided.")

    geos = [resolve_country_to_iso2(t, aliases=aliases) for t in tokens]

    # de-duplicate keep order
    seen = set()
    out = []
    for g in geos:
        if g not in seen:
            out.append(g)
            seen.add(g)
    return out


def main() -> None:
    print("=" * 44)
    print("📊 MACRO STRATEGY ENGINE — Interactive Menu")
    print("=" * 44)

    print("\n⚠️ IMPORTANT: Close any generated Excel files before running.")
    input("Press ENTER to continue...")

    profiles_cfg = load_config("config/profiles.yaml")
    profiles = profiles_cfg.get("profiles", {}) or {}
    profile_key = choose_profile(profiles)
    profile = profiles[profile_key]

    frameworks_cfg = load_config(profile["frameworks_path"])
    frameworks = frameworks_cfg.get("frameworks", {}) or {}

    # aliases por perfil (o común)
    aliases = load_country_aliases(profile.get("aliases_path", "config/country_aliases.yaml"))

    # Countries
    while True:
        try:
            inp = input("\n🌍 Enter countries (comma separated) e.g. España, France, DEU: ")
            geos = parse_countries_with_aliases(inp, aliases)
            break
        except Exception as e:
            print(f"❌ {e}")

    print(f"✅ Normalized countries (ISO2): {geos}")

    # Frameworks
    while True:
        try:
            selected_frameworks = choose_frameworks(frameworks)
            break
        except Exception as e:
            print(f"❌ {e}")

    # AI
    enable_ai = ask_yes_no("\n🤖 Enable AI executive briefings?", default=True)

    # Outputs
    output_flags = choose_outputs()

    # Output folder
    out_dir = choose_output_dir()
    out_dir = out_dir / profile_key
    out_dir.mkdir(parents=True, exist_ok=True)

    # Summary
    print("\n🧾 Summary")
    print(f"  Countries: {geos}")
    print(f"  Frameworks: {selected_frameworks}")
    print(f"  AI enabled: {enable_ai}")
    print(f"  Profile: {profile_key}")
    print(f"  Output dir: {out_dir.resolve()}")
    print(f"  Outputs: {output_flags}")

    if not ask_yes_no("\nRun now?", default=True):
        print("Cancelled.")
        return

    # ---- Call engine ----
    from run import run_engine

    run_engine(
        geos=geos,
        selected_frameworks=selected_frameworks,
        output_dir=out_dir,
        enable_ai=enable_ai,
        output_flags=output_flags,
        frameworks_path=profile["frameworks_path"],
        prompts_path=profile["prompts_path"],
    )

    print("\n✅ Done! 🎉")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)