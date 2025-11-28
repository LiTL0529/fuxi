"""Utilities for extracting dataset definitions from ESGF wget scripts."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Iterable, List
import shlex

DOWNLOAD_MARKER = "EOF--dataset.file.url.chksum_type.chksum"
CAT_DIRECTIVE = f"<<{DOWNLOAD_MARKER}"


@dataclass(slots=True)
class DatasetEntry:
    """Represents a single NetCDF file entry inside the script."""

    filename: str
    url: str
    checksum_type: str | None = None
    checksum: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def extract_entries(script_text: str) -> List[DatasetEntry]:
    """Return all dataset entries declared in the embedded here-document."""

    block_lines = _read_download_block(script_text)
    entries: List[DatasetEntry] = []
    for raw_line in block_lines:
        cleaned = raw_line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        tokens = shlex.split(cleaned)
        if len(tokens) < 2:
            continue
        filename, url = tokens[0], tokens[1]
        checksum_type = tokens[2] if len(tokens) > 2 else None
        checksum = tokens[3] if len(tokens) > 3 else None
        entries.append(
            DatasetEntry(
                filename=_strip_quotes(filename),
                url=_strip_quotes(url),
                checksum_type=checksum_type.lower() if checksum_type else None,
                checksum=_strip_quotes(checksum) if checksum else None,
            )
        )
    if not entries:
        raise ValueError("No dataset entries found inside the script")
    return entries


def _strip_quotes(value: str) -> str:
    return value.strip("'\"")


def _read_download_block(script_text: str) -> Iterable[str]:
    lines = script_text.splitlines()
    inside = False
    block: List[str] = []
    for raw_line in lines:
        stripped = raw_line.strip()
        if not inside and CAT_DIRECTIVE in stripped:
            inside = True
            continue
        if inside:
            if stripped == DOWNLOAD_MARKER:
                break
            block.append(raw_line)
    if not block:
        raise ValueError("Could not locate download block; check script format")
    return block
