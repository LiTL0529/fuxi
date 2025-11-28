"""Async helpers for downloading NetCDF files and packaging them."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple, Callable, Awaitable
import asyncio
import hashlib
import zipfile

import httpx

from .parser import DatasetEntry

_SUPPORTED_HASHES = {"sha256", "md5"}
_CHUNK_SIZE = 1 << 20  # 1 MiB


ProgressCallback = Callable[[DatasetEntry, Path], Awaitable[None] | None]


async def collect_and_package(
    entries: Iterable[DatasetEntry],
    workdir: Path,
    concurrency: int = 4,
    progress_callback: ProgressCallback | None = None,
) -> Tuple[Path, List[Path]]:
    """Download all datasets and package them into a zip archive."""

    workdir.mkdir(parents=True, exist_ok=True)
    data_dir = workdir / "nc"
    data_dir.mkdir(parents=True, exist_ok=True)

    semaphore = asyncio.Semaphore(max(1, concurrency))
    downloaded_paths: List[Path] = []

    async with httpx.AsyncClient(follow_redirects=True, timeout=90) as client:
        async def _task(entry: DatasetEntry) -> Path:
            async with semaphore:
                path = await _download_one(client, entry, data_dir)
                if progress_callback:
                    result = progress_callback(entry, path)
                    if asyncio.iscoroutine(result):
                        await result
                return path

        downloaded_paths = await asyncio.gather(*(_task(entry) for entry in entries))

    archive_path = workdir / "nc_bundle.zip"
    _write_archive(archive_path, downloaded_paths, workdir)
    return archive_path, downloaded_paths


async def _download_one(client: httpx.AsyncClient, entry: DatasetEntry, data_dir: Path) -> Path:
    target_path = data_dir / entry.filename
    target_path.parent.mkdir(parents=True, exist_ok=True)

    hasher = _build_hasher(entry)
    async with client.stream("GET", entry.url) as response:
        response.raise_for_status()
        with target_path.open("wb") as file_obj:
            async for chunk in response.aiter_bytes(_CHUNK_SIZE):
                file_obj.write(chunk)
                if hasher:
                    hasher.update(chunk)

    if hasher and entry.checksum:
        digest = hasher.hexdigest().lower()
        if digest != entry.checksum.lower():
            target_path.unlink(missing_ok=True)
            raise ValueError(
                f"Checksum mismatch for {entry.filename}: expected {entry.checksum}, got {digest}"
            )
    return target_path


def _build_hasher(entry: DatasetEntry) -> hashlib._Hash | None:
    if not entry.checksum_type:
        return None
    algo = entry.checksum_type.lower()
    if algo not in _SUPPORTED_HASHES:
        raise ValueError(f"Unsupported checksum type: {entry.checksum_type}")
    return hashlib.new(algo)


def _write_archive(archive_path: Path, files: Iterable[Path], workdir: Path) -> None:
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in files:
            archive.write(file, arcname=file.name)

