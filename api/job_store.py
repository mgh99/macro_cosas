from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_JOBS_DIR = Path(__file__).resolve().parent.parent / "outputs" / "api_jobs"

_DT_FIELDS = ("created_at", "started_at", "completed_at")


def _job_path(job_id: str) -> Path:
    return _JOBS_DIR / job_id / "job.json"


def _write(job: Dict[str, Any]) -> None:
    path = _job_path(job["job_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(job, default=str), encoding="utf-8")


def _read(job_id: str) -> Optional[Dict[str, Any]]:
    path = _job_path(job_id)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    for field in _DT_FIELDS:
        if data.get(field):
            data[field] = datetime.fromisoformat(data[field])
    return data


def create_job(params: Dict[str, Any]) -> str:
    job_id = str(uuid.uuid4())
    job: Dict[str, Any] = {
        "job_id": job_id,
        "status": "pending",
        "params": params,
        "created_at": datetime.utcnow().isoformat(),
        "started_at": None,
        "completed_at": None,
        "error": None,
        "output_dir": None,
        "output_files": [],
    }
    _write(job)
    return job_id


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return _read(job_id)


def list_jobs() -> List[Dict[str, Any]]:
    if not _JOBS_DIR.exists():
        return []
    jobs = []
    for d in _JOBS_DIR.iterdir():
        if d.is_dir():
            job = _read(d.name)
            if job:
                jobs.append(job)
    return sorted(jobs, key=lambda j: j.get("created_at") or "", reverse=True)


def update_job(job_id: str, **kwargs: Any) -> None:
    job = _read(job_id)
    if job is None:
        return
    # Convert datetimes to ISO strings for JSON serialization
    for k, v in kwargs.items():
        if isinstance(v, datetime):
            kwargs[k] = v.isoformat()
    job.update(kwargs)
    _write(job)
