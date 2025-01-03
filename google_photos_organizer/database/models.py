"""Data models for database operations."""

from dataclasses import dataclass
from enum import Enum


class PhotoSource(str, Enum):
    """Photo source enum."""

    LOCAL = "local"
    GOOGLE = "google"


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
    path: str


@dataclass
class GooglePhotoData(BasePhotoData):
    """Data class for Google Photos information."""


@dataclass
class LocalPhotoData(BasePhotoData):
    """Data class for local photo information."""


@dataclass
class GoogleAlbumData:
    """Data class for Google Photos album information."""

    id: str
    title: str
    creation_time: str


@dataclass
class LocalAlbumData:
    """Data class for local album information."""

    id: str
    title: str
    path: str
    creation_time: str
