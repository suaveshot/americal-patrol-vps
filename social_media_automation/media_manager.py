"""
Americal Patrol — Social Media Media Manager
Handles image storage, library catalog, and cleanup.

Image priority:
  1. Real photos from Google Drive folder (uploaded by Don/Sam)
  2. Previously generated AI images from the library (matching tags)
  3. Freshly generated AI images

Catalog: image_library.json tracks all images with tags for reuse.
"""

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR    = Path(__file__).parent
MEDIA_DIR     = SCRIPT_DIR / "media"
REAL_DIR      = MEDIA_DIR / "real"
GENERATED_DIR = MEDIA_DIR / "generated"
LIBRARY_FILE  = SCRIPT_DIR / "image_library.json"


def _ensure_dirs():
    MEDIA_DIR.mkdir(exist_ok=True)
    REAL_DIR.mkdir(exist_ok=True)
    GENERATED_DIR.mkdir(exist_ok=True)


def _load_library() -> list[dict]:
    if LIBRARY_FILE.exists():
        with open(LIBRARY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def _save_library(library: list[dict]):
    with open(LIBRARY_FILE, "w", encoding="utf-8") as f:
        json.dump(library, f, indent=2)


def save_image(image_bytes: bytes, filename: str, tags: list[str],
               source: str = "generated") -> Path:
    """
    Save an image to the media directory and catalog it.

    Args:
        image_bytes: Raw image data
        filename: Filename for the image
        tags: List of descriptive tags for catalog
        source: "generated" or "real"

    Returns:
        Path to the saved image file
    """
    _ensure_dirs()
    target_dir = REAL_DIR if source == "real" else GENERATED_DIR
    filepath = target_dir / filename

    with open(filepath, "wb") as f:
        f.write(image_bytes)

    library = _load_library()
    library.append({
        "filename": filename,
        "path": str(filepath),
        "tags": tags,
        "source": source,
        "created_at": datetime.now().isoformat(),
        "used_count": 0,
    })
    _save_library(library)

    return filepath


def find_matching_image(tags: list[str], prefer_real: bool = True) -> dict | None:
    """
    Find a previously saved image that matches the given tags.
    Prefers real photos over generated images.
    Returns the library entry dict, or None if no match.
    """
    library = _load_library()
    if not library or not tags:
        return None

    tag_set = set(t.lower() for t in tags)

    scored = []
    for entry in library:
        entry_tags = set(t.lower() for t in entry.get("tags", []))
        overlap = len(tag_set & entry_tags)
        if overlap == 0:
            continue

        # Check file still exists
        if not Path(entry["path"]).exists():
            continue

        # Score: overlap count + bonus for real photos + penalty for overuse
        score = overlap
        if prefer_real and entry.get("source") == "real":
            score += 10
        score -= entry.get("used_count", 0) * 0.5

        scored.append((score, entry))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def mark_image_used(filename: str):
    """Increment the used_count for an image in the library."""
    library = _load_library()
    for entry in library:
        if entry["filename"] == filename:
            entry["used_count"] = entry.get("used_count", 0) + 1
            break
    _save_library(library)


def get_image_path(filename: str) -> Path | None:
    """Get the full path for a cataloged image."""
    library = _load_library()
    for entry in library:
        if entry["filename"] == filename:
            path = Path(entry["path"])
            if path.exists():
                return path
    return None


def cleanup_old_media(days: int = 30) -> int:
    """
    Delete generated images older than N days that haven't been cataloged
    as favorites. Real photos are never deleted.
    Returns the number of files cleaned up.
    """
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0
    library = _load_library()

    keep_files = set()
    remaining = []

    for entry in library:
        path = Path(entry["path"])
        if entry.get("source") == "real":
            remaining.append(entry)
            keep_files.add(path.name)
            continue

        try:
            created = datetime.fromisoformat(entry["created_at"])
            if created < cutoff and entry.get("used_count", 0) < 3:
                if path.exists():
                    path.unlink()
                    deleted += 1
            else:
                remaining.append(entry)
                keep_files.add(path.name)
        except (ValueError, KeyError):
            remaining.append(entry)
            keep_files.add(path.name)

    _save_library(remaining)
    return deleted
