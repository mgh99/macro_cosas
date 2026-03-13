from __future__ import annotations

"""
Macro Strategy Engine — FastAPI Backend
Run with:
    uvicorn api.main:app --reload
"""

import sys
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Ensure repo root is importable (connectors, core, run, ai, ...)
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

load_dotenv(_REPO_ROOT / ".env")

from api.routers import countries, jobs, profiles, settings  # noqa: E402

app = FastAPI(
    title="Macro Strategy Engine API",
    description="REST API for the Macro Strategy Engine — fetch, transform and analyze macroeconomic data.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(profiles.router, prefix="/api")
app.include_router(jobs.router, prefix="/api")
app.include_router(countries.router, prefix="/api")
app.include_router(settings.router, prefix="/api")


@app.get("/api/health", tags=["health"])
def health():
    return {"status": "ok"}


# ── Serve frontend ──
_FRONTEND = _REPO_ROOT / "frontend"

# These routes must be declared BEFORE app.mount() so they take precedence
@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(str(_FRONTEND / "dashboard.html"))


@app.get("/v2", include_in_schema=False)
def dashboard_v2():
    return FileResponse(str(_FRONTEND / "dashboard_v2.html"))


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse(str(_FRONTEND / "favicon.ico"), media_type="image/x-icon")


# Static mount last — serves /static/* (js, css, other assets)
app.mount("/static", StaticFiles(directory=str(_FRONTEND)), name="static")
