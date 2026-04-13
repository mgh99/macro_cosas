from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Profile(str, Enum):
    europe = "europe"
    world = "world"


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class OutputFlags(BaseModel):
    csv: bool = True
    excel_by_indicator: bool = False
    single_sheet: bool = True


class JobRequest(BaseModel):
    profile: Profile
    geos: List[str] = Field(..., min_length=1, description="List of country names or ISO2/ISO3 codes")
    nuts3_geos: Optional[List[str]] = Field(None, description="NUTS3 region codes (e.g. ES300)")
    frameworks: Optional[List[str]] = Field(None, description="Framework keys to run; null = all")
    enable_ai: bool = Field(False, description="Generate Mistral AI executive briefings")
    output_flags: OutputFlags = Field(default_factory=OutputFlags)


class JobSubmitted(BaseModel):
    job_id: str
    status: JobStatus
    created_at: datetime


class JobProgress(BaseModel):
    framework: str
    indicator_idx: int
    indicator_total: int
    indicator_name: str
    source: str


class JobDetail(BaseModel):
    job_id: str
    status: JobStatus
    params: Dict[str, Any]
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    output_files: List[str] = []
    progress: Optional[JobProgress] = None
    fetch_errors: List[Dict[str, Any]] = []


class FrameworkInfo(BaseModel):
    key: str
    label: Optional[str] = None
    indicator_count: int


class ProfileInfo(BaseModel):
    profile: str
    frameworks: List[FrameworkInfo]


class CountryResolveRequest(BaseModel):
    profile: Profile
    countries: List[str]


class CountryResolveResponse(BaseModel):
    resolved: Dict[str, Optional[str]]
    errors: Dict[str, str]
