"""Models for Google Photos Organizer."""

from dataclasses import dataclass
from typing import Dict, List, Any

@dataclass
class MediaItem:
    """Represents a media item in Google Photos."""
    id: str
    filename: str
    mime_type: str
    product_url: str
    metadata: Dict[str, Any]

@dataclass
class Album:
    """Represents an album in Google Photos."""
    id: str
    title: str
    media_items: List[MediaItem]

class GooglePhotosError(Exception):
    """Base exception for Google Photos operations."""

class AuthenticationError(GooglePhotosError):
    """Raised when authentication fails."""

class ApiError(GooglePhotosError):
    """Raised when API calls fail."""
