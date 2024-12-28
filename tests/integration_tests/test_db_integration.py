"""Integration tests for database operations."""
import pytest
from datetime import datetime

from google_photos_organizer.database.models import (
    GoogleAlbumData,
    GooglePhotoData,
    PhotoSource
)

@pytest.mark.integration
def test_photo_album_integration(db_manager):
    """Test the integration between photos and albums."""
    # Initialize database tables
    db_manager.init_database()

    # Create test data
    current_time = datetime.now().isoformat()

    album = GoogleAlbumData(
        id="test_album_1",
        title="Test Album",
        creation_time=current_time
    )

    photo = GooglePhotoData(
        id="test_photo_1",
        filename="test.jpg",
        normalized_filename="test.jpg",
        creation_time=current_time,
        mime_type="image/jpeg",
        width=1920,
        height=1080,
        product_url="http://example.com/photo1"
    )

    # Test database operations
    db_manager.store_album(album, PhotoSource.GOOGLE)
    db_manager.store_photo(photo, PhotoSource.GOOGLE)
    db_manager.store_album_photo(album.id, photo.id, PhotoSource.GOOGLE)

    # Test retrieval
    retrieved_album = db_manager.get_album(album.title, PhotoSource.GOOGLE)
    assert retrieved_album is not None
    assert retrieved_album['id'] == album.id
    assert retrieved_album['title'] == album.title

    # Test search
    search_results = db_manager.search_photos("test.jpg", "test.jpg")
    assert len(search_results) > 0
    assert search_results[0][1] == photo.filename  # filename is the second column
    assert search_results[0][7] == "Test Album"  # album title is now in the last column
