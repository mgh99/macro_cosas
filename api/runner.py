from __future__ import annotations

"""
Runs run_engine() in a background thread and updates job_store accordingly.
"""

import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

# Ensure repo root is on sys.path so `run`, `core`, `connectors`, etc. are importable
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def execute_job(job_id: str) -> None:
    """
    Entry point called from a ThreadPoolExecutor.
    Reads job params from job_store, runs run_engine, and writes results back.
    """
    from api.job_store import get_job, update_job
    import run as engine

    job = get_job(job_id)
    if job is None:
        return

    params: Dict[str, Any] = job["params"]
    profile: str = params["profile"]

    frameworks_path = str(_REPO_ROOT / "config" / "profiles" / profile / "frameworks.yaml")
    prompts_path = str(_REPO_ROOT / "config" / "profiles" / profile / "prompts.yaml")
    output_dir = _REPO_ROOT / "outputs" / "api_jobs" / job_id
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save output_dir immediately so the frontend can find files even if we fail mid-run
    update_job(job_id, status="running", started_at=datetime.utcnow(), output_dir=str(output_dir))

    def _collect_files() -> list[str]:
        return [
            f.relative_to(output_dir).as_posix()
            for f in output_dir.rglob("*")
            if f.is_file()
        ]

    def _progress_callback(fw_name, idx, total, ind_name, source):
        update_job(job_id, progress={
            "framework": fw_name,
            "indicator_idx": idx,
            "indicator_total": total,
            "indicator_name": ind_name,
            "source": source,
        })

    try:
        engine.run_engine(
            geos=params["geos"],
            nuts3_geos=params.get("nuts3_geos"),
            selected_frameworks=params.get("frameworks"),
            output_dir=output_dir,
            enable_ai=params.get("enable_ai", False),
            output_flags={
                "csv": params.get("output_flags", {}).get("csv", True),
                "excel_by_indicator": params.get("output_flags", {}).get("excel_by_indicator", False),
                "single_sheet": params.get("output_flags", {}).get("single_sheet", True),
                "debug_no_files": False,
            },
            frameworks_path=frameworks_path,
            prompts_path=prompts_path,
            progress_callback=_progress_callback,
        )

        update_job(
            job_id,
            status="completed",
            completed_at=datetime.utcnow(),
            output_files=_collect_files(),
        )

    except Exception:
        # Collect any files written before the crash so the user can still download them
        update_job(
            job_id,
            status="failed",
            completed_at=datetime.utcnow(),
            error=traceback.format_exc(),
            output_files=_collect_files(),
        )
