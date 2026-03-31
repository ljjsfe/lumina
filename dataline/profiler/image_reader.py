"""Image file profiling (metadata only, no vision)."""

from __future__ import annotations

import os

from ..core.types import ManifestEntry


def read_image(file_path: str) -> ManifestEntry:
    """Profile an image file into a ManifestEntry."""
    size = os.path.getsize(file_path)
    ext = os.path.splitext(file_path)[1].lower()

    try:
        from PIL import Image
        with Image.open(file_path) as img:
            return ManifestEntry(
                file_path=file_path, file_type="image", size_bytes=size,
                summary={
                    "dimensions": list(img.size),
                    "file_format": img.format or ext.lstrip("."),
                    "mode": img.mode,
                },
            )
    except Exception as e:
        return ManifestEntry(
            file_path=file_path, file_type="image", size_bytes=size,
            summary={"error": str(e), "file_format": ext.lstrip(".")},
        )
