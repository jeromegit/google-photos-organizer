"""Data models for database operations."""

from dataclasses import dataclass


@dataclass
class BasePhotoData:
    """Base class for photo information."""
    id: str
    filename: str
    normalized_filename: str
    creation_time: str
    mime_type: str
    width: int
    height: int


@dataclass
class PhotoData(BasePhotoData):
    """Data class for Google Photos information."""
    product_url: str


@dataclass
class LocalPhotoData(BasePhotoData):
    """Data class for local photo information."""
    full_path: str
    size: int


@dataclass
class AlbumData:
    """Data class for album information."""
    id: str
    title: str
    creation_time: str


@dataclass
class LocalAlbumData:
    """Data class for local album information."""
    id: str
    title: str
    full_path: str
    creation_time: str
