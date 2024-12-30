"""Unit tests for file utilities."""

from pathlib import Path

import pytest
from PIL import Image

from google_photos_organizer.utils.file_utils import (
    get_file_metadata,
    get_image_dimensions,
    normalize_filename,
)


def test_normalize_filename():
    """Test filename normalization."""
    test_cases = [
        ("Photo 1.jpg", "photo1"),
        ("My-Photo_2.PNG", "myphoto2"),
        ("Vacation!2023.jpeg", "vacation2023"),
        ("test@#$%^&*.gif", "test"),
        ("UPPER_CASE.jpg", "uppercase"),
    ]

    for input_name, expected in test_cases:
        assert normalize_filename(input_name) == expected


@pytest.fixture
def test_image(tmp_path):
    """Create a test image file."""
    image_path = tmp_path / "test_image.jpg"
    # Create a small test image
    img = Image.new("RGB", (100, 200), color="red")
    img.save(image_path)
    return image_path


def test_get_image_dimensions(test_image):
    """Test getting image dimensions."""
    width, height = get_image_dimensions(str(test_image))
    assert width == 100
    assert height == 200

    # Test with non-existent file
    width, height = get_image_dimensions("non_existent.jpg")
    assert width == 0
    assert height == 0

    # Test with non-image file
    text_file = Path(test_image).parent / "test.txt"
    text_file.write_text("Not an image")
    width, height = get_image_dimensions(str(text_file))
    assert width == 0
    assert height == 0


def test_get_file_metadata(test_image):
    """Test getting file metadata."""
    metadata = get_file_metadata(str(test_image))
    assert metadata is not None
    assert metadata.filename == "test_image.jpg"
    assert metadata.mime_type == "image/jpeg"
    assert metadata.size > 0
    assert metadata.width == 100
    assert metadata.height == 200
    assert isinstance(metadata.creation_time, str)
    assert isinstance(metadata.modified, str)

    # Test with non-existent file
    assert get_file_metadata("non_existent.jpg") is None

    # Test with directory
    assert get_file_metadata(str(Path(test_image).parent)) is None
