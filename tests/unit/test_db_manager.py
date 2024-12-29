"""Test module for database manager functionality."""

from pathlib import Path
from typing import Generator

import pytest

from google_photos_organizer.database.db_manager import DatabaseManager
from google_photos_organizer.database.models import (
    GoogleAlbumData,
    GooglePhotoData,
    LocalAlbumData,
    LocalPhotoData,
    PhotoSource,
)


@pytest.fixture(scope="function")
def test_db_manager(tmp_path: Path) -> Generator[DatabaseManager, None, None]:
    """Create a test database manager."""
    db_path = tmp_path / "test.db"
    manager = DatabaseManager(str(db_path))
    yield manager
    if db_path.exists():
        db_path.unlink()


def test_init_database(test_db_manager):
    """Test database initialization."""
    # Initialize database for both sources
    for source in PhotoSource:
        test_db_manager.init_database(source=source)

    # Verify tables exist
    tables = test_db_manager.list_tables()
    for source in PhotoSource:
        prefix = f"{source.value}_"
        assert f"{prefix}photos" in tables
        assert f"{prefix}albums" in tables
        assert f"{prefix}album_photos" in tables


def test_store_google_photo(test_db_manager):
    """Test storing Google photo data."""
    test_db_manager.init_database(source=PhotoSource.GOOGLE)

    photo = GooglePhotoData(
        id="test_id",
        filename="test.jpg",
        normalized_filename="test_jpg",
        mime_type="image/jpeg",
        creation_time="2023-01-01T00:00:00Z",
        width=100,
        height=100,
        path="",
    )

    test_db_manager.store_photo(photo, PhotoSource.GOOGLE)
    count = test_db_manager.count_photos(PhotoSource.GOOGLE)
    assert count == 1


def test_store_local_photo(test_db_manager):
    """Test storing local photo data."""
    test_db_manager.init_database(source=PhotoSource.LOCAL)

    photo = LocalPhotoData(
        id="test_id",
        filename="test.jpg",
        normalized_filename="test_jpg",
        mime_type="image/jpeg",
        creation_time="2023-01-01T00:00:00Z",
        width=100,
        height=100,
        path="/path/to/photo.jpg",
        size=1024,
    )

    test_db_manager.store_photo(photo, PhotoSource.LOCAL)
    count = test_db_manager.count_photos(PhotoSource.LOCAL)
    assert count == 1


def test_store_google_album(test_db_manager):
    """Test storing Google album data."""
    test_db_manager.init_database(source=PhotoSource.GOOGLE)

    album = GoogleAlbumData(
        id="test_album_id", title="Test Album", creation_time="2023-01-01T00:00:00Z"
    )

    test_db_manager.store_album(album, PhotoSource.GOOGLE)
    result = test_db_manager.get_album("Test Album", PhotoSource.GOOGLE)
    assert result is not None
    assert result["id"] == "test_album_id"


def test_store_local_album(test_db_manager):
    """Test storing local album data."""
    test_db_manager.init_database(source=PhotoSource.LOCAL)

    album = LocalAlbumData(
        id="test_album_id",
        title="Test Album",
        creation_time="2023-01-01T00:00:00Z",
        path="/path/to/album",
    )

    test_db_manager.store_album(album, PhotoSource.LOCAL)
    result = test_db_manager.get_album("Test Album", PhotoSource.LOCAL)
    assert result is not None
    assert result["id"] == "test_album_id"


def test_search_photos(test_db_manager):
    """Test searching photos across sources."""
    # Initialize both sources
    for source in PhotoSource:
        test_db_manager.init_database(source=source)

    # Add a Google photo and album
    google_photo = GooglePhotoData(
        id="google_id",
        filename="vacation.jpg",
        normalized_filename="vacation_jpg",
        mime_type="image/jpeg",
        creation_time="2023-01-01T00:00:00Z",
        width=100,
        height=100,
        path="",
    )
    test_db_manager.store_photo(google_photo, PhotoSource.GOOGLE)

    google_album = GoogleAlbumData(
        id="google_album_id", title="Summer Vacation", creation_time="2023-01-01T00:00:00Z"
    )
    test_db_manager.store_album(google_album, PhotoSource.GOOGLE)
    test_db_manager.store_album_photo(google_album.id, google_photo.id, PhotoSource.GOOGLE)

    # Add a local photo and album
    local_photo = LocalPhotoData(
        id="local_id",
        filename="vacation2.jpg",
        normalized_filename="vacation2_jpg",
        mime_type="image/jpeg",
        creation_time="2023-01-01T00:00:00Z",
        width=100,
        height=100,
        path="/path/to/photo.jpg",
        size=1024,
    )
    test_db_manager.store_photo(local_photo, PhotoSource.LOCAL)

    local_album = LocalAlbumData(
        id="local_album_id",
        title="Local Vacation",
        creation_time="2023-01-01T00:00:00Z",
        path="/path/to/album",
    )
    test_db_manager.store_album(local_album, PhotoSource.LOCAL)
    test_db_manager.store_album_photo(local_album.id, local_photo.id, PhotoSource.LOCAL)

    # Search for photos
    results = test_db_manager.search_photos("vacation", "vacation")
    assert len(results) == 2

    # Verify Google photo result
    google_result = next(r for r in results if r[0] == "google")
    assert google_result[1] == "vacation.jpg"  # filename
    assert google_result[7] == "Summer Vacation"  # album name

    # Verify Local photo result
    local_result = next(r for r in results if r[0] == "local")
    assert local_result[1] == "vacation2.jpg"  # filename
    assert local_result[7] == "Local Vacation"  # album name
