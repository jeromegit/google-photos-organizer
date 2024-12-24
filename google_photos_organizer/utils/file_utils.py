"""File utilities for Google Photos Organizer."""

import logging
import mimetypes
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, Any

from PIL import Image

logger = logging.getLogger(__name__)

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
    name = re.sub(r'[^a-z0-9]', '', name)
    return name

def get_file_metadata(file_path: str) -> Optional[Dict[str, Any]]:
    """Get metadata for a file.

    Args:
        file_path: Path to file

    Returns:
        Dictionary containing file metadata, or None if file is not an image
    """
    try:
        path = Path(file_path)
        if not path.is_file():
            return None

        stat = path.stat()
        creation_time = datetime.fromtimestamp(stat.st_ctime)

        mime_type, _ = mimetypes.guess_type(file_path)
        width, height = get_image_dimensions(file_path)
        
        return {
            'filename': os.path.basename(file_path),
            'creation_time': creation_time.isoformat(),
            'size': stat.st_size,
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'mime_type': mime_type or 'application/octet-stream',
            'width': width,
            'height': height
        }
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
