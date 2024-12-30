"""Unit tests for GooglePhotosOrganizer class."""

import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from google_photos_organizer.database.models import GooglePhotoData, LocalPhotoData, PhotoSource
from google_photos_organizer.main import GooglePhotosOrganizer
from google_photos_organizer.utils.file_utils import FileMetadata


@pytest.fixture
def organizer():
    """Create a GooglePhotosOrganizer instance for testing."""
    return GooglePhotosOrganizer(local_photos_dir="/test/photos")


@pytest.fixture
def mock_service():
    """Create a mock Google Photos service."""
    service = MagicMock()
    service.mediaItems.return_value.list.return_value.execute.return_value = {
        "mediaItems": [
            {
                "id": "test_id_1",
                "filename": "test1.jpg",
                "mimeType": "image/jpeg",
                "mediaMetadata": {
                    "creationTime": "2024-01-01T00:00:00Z",
                    "width": "1920",
                    "height": "1080",
                },
            }
        ],
        "nextPageToken": None,
    }
    return service


def test_store_photo_metadata(organizer):
    """Test storing Google photo metadata."""
    photo_id = "test_id"
    filename = "test.jpg"
    normalized_filename = "test_jpg"
    mime_type = "image/jpeg"
    creation_time = "2024-01-01T00:00:00Z"
    width = 1920
    height = 1080

    # Mock the database manager
    organizer.db = MagicMock()

    # Create photo data
    photo_data = GooglePhotoData(
        id=photo_id,
        filename=filename,
        normalized_filename=normalized_filename,
        mime_type=mime_type,
        creation_time=creation_time,
        width=width,
        height=height,
        path=photo_id,
    )

    # Store the photo metadata
    organizer.store_photo_metadata(photo_data=photo_data, source=PhotoSource.GOOGLE)

    # Verify the database call
    organizer.db.store_photo.assert_called_once()
    args = organizer.db.store_photo.call_args[0]
    stored_photo_data = args[0]
    source = args[1]

    assert stored_photo_data.id == photo_id
    assert stored_photo_data.filename == filename
    assert stored_photo_data.normalized_filename == normalized_filename
    assert stored_photo_data.mime_type == mime_type
    assert stored_photo_data.creation_time == creation_time
    assert stored_photo_data.width == width
    assert stored_photo_data.height == height
    assert stored_photo_data.path == photo_id
    assert source == PhotoSource.GOOGLE


def test_store_local_photo_metadata(organizer):
    """Test storing local photo metadata."""
    photo_id = "test_id"
    filename = "test.jpg"
    normalized_filename = "test_jpg"
    path = "/test/photos/test.jpg"
    creation_time = "2024-01-01T00:00:00Z"
    width = 1920
    height = 1080
    mime_type = "image/jpeg"

    # Mock the database manager
    organizer.db = MagicMock()

    # Create photo data
    photo_data = LocalPhotoData(
        id=photo_id,
        filename=filename,
        normalized_filename=normalized_filename,
        path=path,
        creation_time=creation_time,
        width=width,
        height=height,
        mime_type=mime_type,
    )

    # Store the photo metadata
    organizer.store_local_photo_metadata(photo_data)

    # Verify the database call
    organizer.db.store_photo.assert_called_once()
    args = organizer.db.store_photo.call_args[0]
    stored_photo_data = args[0]
    source = args[1]

    assert stored_photo_data.id == photo_id
    assert stored_photo_data.filename == filename
    assert stored_photo_data.normalized_filename == normalized_filename
    assert stored_photo_data.path == path
    assert stored_photo_data.creation_time == creation_time
    assert stored_photo_data.width == width
    assert stored_photo_data.height == height
    assert stored_photo_data.mime_type == mime_type
    assert source == PhotoSource.LOCAL


@patch("google_photos_organizer.main.get_file_metadata")
@patch("google_photos_organizer.main.get_image_dimensions")
@patch("google_photos_organizer.main.normalize_filename")
def test_scan_local_directory(mock_normalize, mock_dimensions, mock_metadata, organizer):
    """Test scanning local directory."""
    # Mock os.walk to return test files
    test_files = [
        ("root", ["dir1"], ["test1.jpg", "test2.png", "ignore.txt"]),
        ("root/dir1", [], ["test3.jpg"]),
    ]
    with (
        patch("os.walk", return_value=test_files),
        patch("os.path.exists", return_value=True),
        patch("os.stat") as mock_stat,
        patch("os.path.join", side_effect=lambda *args: "/".join(args)),
        patch(
            "google_photos_organizer.main.is_media_file",
            side_effect=lambda f: f.endswith((".jpg", ".png")),
        ),
    ):
        # Mock file metadata
        def get_mock_metadata(filepath):
            return FileMetadata(
                filename=os.path.basename(filepath),
                creation_time="2024-01-01T00:00:00Z",
                size=1024,
                modified="2024-01-01T00:00:00Z",
                mime_type="image/jpeg",
                width=1920,
                height=1080,
            )

        mock_metadata.side_effect = get_mock_metadata
        mock_dimensions.return_value = (1920, 1080)
        mock_normalize.side_effect = lambda x: x.replace(".", "_")
        mock_stat.return_value.st_mtime = datetime.now().timestamp()

        # Mock database manager
        organizer.db = MagicMock()

        # Run the scan
        organizer.scan_local_directory()

        # Verify that only image files were processed
        assert mock_metadata.call_count == 3  # test1.jpg, test2.png, test3.jpg
        assert organizer.db.store_photo.call_count == 3


def test_store_photos(organizer, mock_service):
    """Test storing Google photos."""
    # Set up the mock service
    organizer.service = mock_service
    organizer.db = MagicMock()

    # Run the store photos function
    result = organizer.store_photos(max_photos=1)

    # Verify the result
    assert result is True
    organizer.db.store_photo.assert_called_once()
    args = organizer.db.store_photo.call_args[0]
    photo_data = args[0]
    source = args[1]

    assert photo_data.id == "test_id_1"
    assert photo_data.filename == "test1.jpg"
    assert photo_data.mime_type == "image/jpeg"
    assert photo_data.creation_time == "2024-01-01T00:00:00Z"
    assert photo_data.width == 1920
    assert photo_data.height == 1080
    assert source == PhotoSource.GOOGLE


def test_store_photos_no_auth(organizer):
    """Test storing photos without authentication."""
    # Don't set the service
    organizer.service = None

    # Run the store photos function
    result = organizer.store_photos()

    # Verify that it fails gracefully
    assert result is False


def test_store_photos_api_error(organizer, mock_service):
    """Test storing photos with API error."""
    # Set up the mock service to raise an error
    organizer.service = mock_service
    organizer.service.mediaItems.return_value.list.return_value.execute.side_effect = Exception(
        "API Error"
    )
    organizer.db = MagicMock()

    # Run the store photos function
    result = organizer.store_photos()

    # Verify that it fails gracefully
    assert result is False
    organizer.db.store_photo.assert_not_called()
