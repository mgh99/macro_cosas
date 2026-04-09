from __future__ import annotations

import sys
from pathlib import Path
from typing import List

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse

from api.job_store import create_job, get_job, list_jobs, update_job
from api.schemas import JobDetail, JobRequest, JobStatus, JobSubmitted

router = APIRouter(prefix="/jobs", tags=["jobs"])

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _resolve_geos(geos: list[str], profile: str) -> list[str]:
    """Convert raw country strings to ISO2 codes, raising 422 if any fail."""
    from core.country_resolver import load_country_aliases, resolve_country_to_iso2

    aliases_path = str(_REPO_ROOT / "config" / "profiles" / profile / "country_aliases.yaml")
    aliases = load_country_aliases(aliases_path)

    resolved = []
    errors = []
    for g in geos:
        try:
            resolved.append(resolve_country_to_iso2(g, aliases))
        except ValueError as e:
            errors.append(str(e))

    if errors:
        raise HTTPException(status_code=422, detail={"country_errors": errors})

    return resolved


def _extra_aggregate_geos(profile: str, framework_names: list[str] | None) -> list[str]:
    """
    Return aggregate geo codes (WEOWORLD, ADVEC, EU) if any indicator in the
    selected frameworks has allow_aggregates: true.  The engine already skips
    these codes for indicators that don't support them.
    """
    import yaml
    from core.country_resolver import AGGREGATE_GEO_CODES

    frameworks_path = _REPO_ROOT / "config" / "profiles" / profile / "frameworks.yaml"
    if not frameworks_path.exists():
        return []

    with open(frameworks_path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}

    # YAML structure: {"frameworks": {"economics": {...}, "demographics": {...}, ...}}
    frameworks_raw = cfg.get("frameworks", {}) or {}
    fw_set = set(framework_names) if framework_names else set(frameworks_raw.keys())

    for fw_key, fw in frameworks_raw.items():
        if fw_key not in fw_set:
            continue
        for ind in (fw.get("indicators") or []):
            if ind.get("allow_aggregates") and ind.get("enabled", True):
                return list(AGGREGATE_GEO_CODES)  # at least one found — add all three

    return []


@router.post("", response_model=JobSubmitted, status_code=202)
def submit_job(req: JobRequest, background_tasks: BackgroundTasks):
    """
    Submit a new analytics run.
    Countries are resolved immediately; the engine runs in the background.
    Returns a job_id to poll for status.
    """
    import json
    from api.runner import execute_job

    print(f"[submit_job] received: {json.dumps(req.model_dump(), default=str)}")
    resolved_geos = _resolve_geos(req.geos, req.profile.value)

    # Automatically add aggregate geo codes (WEOWORLD, ADVEC, EU) when any
    # selected framework contains indicators that require them (allow_aggregates: true).
    for ag in _extra_aggregate_geos(req.profile.value, req.frameworks):
        if ag not in resolved_geos:
            resolved_geos.append(ag)

    params = {
        "profile": req.profile.value,
        "geos": resolved_geos,
        "nuts3_geos": req.nuts3_geos,
        "frameworks": req.frameworks,
        "enable_ai": req.enable_ai,
        "output_flags": req.output_flags.model_dump(),
    }

    job_id = create_job(params)
    background_tasks.add_task(execute_job, job_id)

    job = get_job(job_id)
    return JobSubmitted(
        job_id=job_id,
        status=JobStatus.pending,
        created_at=job["created_at"],
    )


@router.get("", response_model=List[JobDetail])
def list_all_jobs():
    """List all submitted jobs."""
    return [_job_to_detail(j) for j in list_jobs()]


@router.get("/{job_id}", response_model=JobDetail)
def get_job_status(job_id: str):
    """Get status and metadata for a specific job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_to_detail(job)


@router.get("/{job_id}/files", response_model=List[str])
def list_job_files(job_id: str):
    """List all output files produced by a completed job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=409, detail=f"Job is '{job['status']}', not completed yet")
    return job.get("output_files", [])


@router.get("/{job_id}/files/{file_path:path}")
def download_job_file(job_id: str, file_path: str):
    """Download a specific output file from a completed job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=409, detail=f"Job is '{job['status']}', not completed yet")

    output_dir = job.get("output_dir")
    if not output_dir:
        raise HTTPException(status_code=404, detail="No output directory for this job")

    full_path = Path(output_dir) / file_path
    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    # Prevent path traversal
    try:
        full_path.resolve().relative_to(Path(output_dir).resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    return FileResponse(
        path=str(full_path),
        filename=full_path.name,
        media_type="application/octet-stream",
    )


def _job_to_detail(job: dict) -> JobDetail:
    from api.schemas import JobProgress
    raw_prog = job.get("progress")
    progress = JobProgress(**raw_prog) if raw_prog else None
    return JobDetail(
        job_id=job["job_id"],
        status=JobStatus(job["status"]),
        params=job["params"],
        created_at=job["created_at"],
        started_at=job.get("started_at"),
        completed_at=job.get("completed_at"),
        error=job.get("error"),
        output_files=job.get("output_files", []),
        progress=progress,
    )
