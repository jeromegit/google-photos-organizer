"""Tests for file utilities."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from google_photos_organizer.utils.file_utils import (
    get_file_metadata,
    get_image_dimensions,
    normalize_filename,
)


def test_normalize_filename():
    """Test normalize_filename function with various inputs."""
    test_cases = [
        ("test.jpg", "test"),
        ("Test File.PNG", "testfile"),
        ("My-Photo_123.jpeg", "myphoto123"),
        ("!@#$%^&*.gif", ""),
        ("UPPER_CASE.jpg", "uppercase"),
        ("spaces   here.png", "spaceshere"),
    ]

    for input_name, expected in test_cases:
        assert normalize_filename(input_name) == expected


def test_get_image_dimensions_valid():
    """Test get_image_dimensions with a valid image file."""
    with patch("PIL.Image.open") as mock_open:
        mock_img = MagicMock()
        mock_img.size = (100, 200)
        mock_open.return_value.__enter__.return_value = mock_img

        dimensions = get_image_dimensions("test.jpg")
        assert dimensions == (100, 200)


def test_get_image_dimensions_invalid():
    """Test get_image_dimensions with an invalid image file."""
    with patch("PIL.Image.open", side_effect=IOError("Invalid image")):
        dimensions = get_image_dimensions("invalid.jpg")
        assert dimensions == (0, 0)


@patch("os.path.isfile")
@patch("os.stat")
@patch("PIL.Image.open")
@patch("mimetypes.guess_type")
def test_get_file_metadata_valid(mock_mime, mock_img_open, mock_stat, mock_isfile):
    """Test get_file_metadata with a valid file."""
    # Setup test data
    test_time = datetime(2024, 12, 24, 15, 13, 20)

    # Mock os.stat
    stat_result = MagicMock()
    stat_result.st_size = 1024
    stat_result.st_ctime = test_time.timestamp()
    stat_result.st_mtime = test_time.timestamp()
    mock_stat.return_value = stat_result

    # Mock os.path.isfile
    mock_isfile.return_value = True

    # Mock mimetypes.guess_type
    mock_mime.return_value = ("image/jpeg", None)

    # Mock PIL.Image.open
    mock_img = MagicMock()
    mock_img.size = (100, 200)
    mock_img_open.return_value.__enter__.return_value = mock_img

    metadata = get_file_metadata("test.jpg")

    assert metadata is not None
    assert metadata["filename"] == "test.jpg"
    assert metadata["size"] == 1024
    assert "creation_time" in metadata
    assert "modified" in metadata
    assert metadata["mime_type"] == "image/jpeg"
    assert metadata["width"] == 100
    assert metadata["height"] == 200


@patch("os.path.isfile")
def test_get_file_metadata_nonexistent(mock_isfile):
    """Test get_file_metadata with a nonexistent file."""
    mock_isfile.return_value = False
    metadata = get_file_metadata("nonexistent.jpg")
    assert metadata is None


@patch("os.path.isfile")
def test_get_file_metadata_error(mock_isfile):
    """Test get_file_metadata with file that raises OSError."""
    mock_isfile.side_effect = OSError("Test error")
    metadata = get_file_metadata("error.jpg")
    assert metadata is None
