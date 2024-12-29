"""File utilities for Google Photos Organizer."""

import logging
import mimetypes
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from PIL import Image

logger = logging.getLogger(__name__)


@dataclass
class FileMetadata:
    """File metadata."""
    filename: str
    creation_time: str
    size: int
    modified: str
    mime_type: str
    width: int
    height: int


def is_media_file(filename: str) -> bool:
    """Check if a file is a media file based on its extension.

    Args:
        filename: Name of the file to check

    Returns:
        True if the file is a media file, False otherwise
    """
    # List of supported media file extensions
    media_extensions = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".bmp",
        ".tiff",
        ".webp",  # Images
        ".mp4",
        ".mov",
        ".avi",
        ".wmv",
        ".flv",
        ".webm",  # Videos
    }
    return os.path.splitext(filename)[1].lower() in media_extensions


def normalize_filename(filename: str) -> str:
    """Normalize filename for comparison.

    Args:
        filename: Filename to normalize

    Returns:
        Normalized filename
    """
    # Remove extension and convert to lowercase
    name = os.path.splitext(filename)[0].lower()
    # Remove special characters and spaces
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def get_file_metadata(file_path: str) -> Optional[FileMetadata]:
    """Get metadata for a file.

    Args:
        file_path: Path to file

    Returns:
        FileMetadata object containing file metadata, or None if file is not an image
    """
    try:
        if not os.path.isfile(file_path):
            return None

        stat = os.stat(file_path)
        creation_time = datetime.fromtimestamp(stat.st_ctime)

        mime_type, _ = mimetypes.guess_type(file_path)
        width, height = get_image_dimensions(file_path)

        return FileMetadata(
            filename=os.path.basename(file_path),
            creation_time=creation_time.isoformat(),
            size=stat.st_size,
            modified=datetime.fromtimestamp(stat.st_mtime).isoformat(),
            mime_type=mime_type or "application/octet-stream",
            width=width,
            height=height,
        )
    except OSError as e:
        logger.warning("Failed to get metadata for %s: %s", file_path, str(e))
        return None


def get_image_dimensions(file_path: str) -> Tuple[int, int]:
    """Get dimensions of an image file.

    Args:
        file_path: Path to image file

    Returns:
        Tuple containing width and height of image

    Raises:
        ValueError: If file is not a valid image
    """
    try:
        with Image.open(file_path) as img:
            return img.size
    except (IOError, OSError) as e:
        logger.warning("Failed to get dimensions for %s: %s", file_path, str(e))
        return (0, 0)
