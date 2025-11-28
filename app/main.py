from __future__ import annotations

import asyncio
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from .parser import DatasetEntry, extract_entries
from .downloader import collect_and_package

BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "templates"
WORK_DIR = Path(os.getenv("NC_WORK_DIR", BASE_DIR.parent / "jobs"))
WORK_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="NC Fetcher", version="0.1.0")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
app.mount("/assets", StaticFiles(directory=str(TEMPLATE_DIR)), name="assets")


class JobState(str):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobRecord:
    job_id: str
    filename: str
    job_dir: Path
    total: int
    completed: int = 0
    status: str = JobState.PENDING
    message: str | None = None
    archive_path: Path | None = None

    def as_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "total": self.total,
            "completed": self.completed,
            "message": self.message,
        }


jobs: Dict[str, JobRecord] = {}


@app.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("upload.html", {"request": request})


@app.post("/inspect")
async def inspect(script: UploadFile = File(...)) -> JSONResponse:
    content = await script.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file uploaded")
    script_text = content.decode("utf-8", errors="ignore")
    entries = extract_entries(script_text)
    return JSONResponse(
        {
            "count": len(entries),
            "datasets": [entry.to_dict() for entry in entries],
        }
    )


@app.post("/upload")
async def upload(
    request: Request,
    script: UploadFile = File(...),
) -> JSONResponse:
    content = await script.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file uploaded")

    script_text = content.decode("utf-8", errors="ignore")
    try:
        entries = extract_entries(script_text)
    except ValueError as exc:  # pragma: no cover - defensive path
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job_id = uuid4().hex
    job_dir = WORK_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    uploaded_copy = job_dir / script.filename
    uploaded_copy.write_bytes(content)

    job = JobRecord(
        job_id=job_id,
        filename=script.filename,
        job_dir=job_dir,
        total=len(entries),
    )
    jobs[job_id] = job
    asyncio.create_task(_run_job(job, entries, job_dir))

    return JSONResponse({"job_id": job_id})


@app.get("/status/{job_id}")
async def status(job_id: str) -> JSONResponse:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(job.as_dict())


@app.get("/download/{job_id}")
async def download(job_id: str, background_tasks: BackgroundTasks) -> FileResponse:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobState.COMPLETED or not job.archive_path:
        raise HTTPException(status_code=400, detail="Job not ready")

    filename = f"{Path(job.filename).stem}_nc_files.zip"
    background_tasks.add_task(_cleanup_job, job_id)
    return FileResponse(job.archive_path, media_type="application/zip", filename=filename, background=background_tasks)


async def _run_job(job: JobRecord, entries: list[DatasetEntry], job_dir: Path) -> None:
    job.status = JobState.RUNNING

    def _progress_callback(entry: DatasetEntry, _: Path) -> None:
        job.completed += 1
        job.message = f"{entry.filename} 完成"

    try:
        archive_path, _ = await collect_and_package(entries, job_dir, progress_callback=_progress_callback)
        job.archive_path = archive_path
        job.status = JobState.COMPLETED
        job.message = "已完成"
    except Exception as exc:  # pragma: no cover
        job.status = JobState.FAILED
        job.message = str(exc)
        shutil.rmtree(job.job_dir, ignore_errors=True)


def _cleanup_job(job_id: str) -> None:
    job = jobs.pop(job_id, None)
    if not job:
        return
    shutil.rmtree(job.job_dir, ignore_errors=True)
