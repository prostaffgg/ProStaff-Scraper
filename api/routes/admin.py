"""
Admin routes — protected endpoints for maintenance operations.
All routes require X-Admin-Token header matching ADMIN_TOKEN env var.
"""

import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

_admin_key_header = APIKeyHeader(name="X-Admin-Token", auto_error=False)

# In-memory store: job_id -> {status, started_at, finished_at, result, error}
_jobs: Dict[str, Dict] = {}


def _require_admin_token(token: Optional[str] = Depends(_admin_key_header)) -> str:
    expected = os.getenv("ADMIN_TOKEN")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="ADMIN_TOKEN is not configured on this server. Set the environment variable.",
        )
    if token != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Admin-Token")
    return token


class OraclesElixirBackfillRequest(BaseModel):
    # None means load all available years from Oracle's Elixir
    years: Optional[List[int]] = None


def _run_backfill_job(job_id: str, years: Optional[List[int]]) -> None:
    # Import here to avoid circular imports and heavy load at startup
    from etl.oracles_elixir_backfill import run_backfill

    _jobs[job_id]["status"] = "running"
    logger.info("[ADMIN] OE backfill job=%s started years=%s", job_id, years or "all")

    try:
        result = run_backfill(years=years, dry_run=False)
        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["result"] = result
        logger.info("[ADMIN] OE backfill job=%s completed result=%s", job_id, result)
    except Exception as exc:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = str(exc)
        logger.error("[ADMIN] OE backfill job=%s failed: %s", job_id, exc)
    finally:
        _jobs[job_id]["finished_at"] = datetime.now(tz=timezone.utc).isoformat()


@router.post("/backfill/oracles-elixir")
def trigger_oracles_elixir_backfill(
    background_tasks: BackgroundTasks,
    body: OraclesElixirBackfillRequest = None,
    _token: str = Depends(_require_admin_token),
):
    if body is None:
        body = OraclesElixirBackfillRequest()

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "started_at": datetime.now(tz=timezone.utc).isoformat(),
        "finished_at": None,
        "result": None,
        "error": None,
        "years": body.years,
    }

    background_tasks.add_task(_run_backfill_job, job_id, body.years)

    logger.info("[ADMIN] OE backfill queued job=%s years=%s", job_id, body.years or "all")

    return {"queued": True, "job_id": job_id}


@router.get("/backfill/oracles-elixir/{job_id}")
def get_oracles_elixir_backfill_status(
    job_id: str,
    _token: str = Depends(_require_admin_token),
):
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "started_at": job["started_at"],
        "finished_at": job["finished_at"],
        "years": job["years"],
        "result": job["result"],
        "error": job["error"],
    }
