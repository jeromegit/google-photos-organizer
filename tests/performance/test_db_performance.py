"""Performance tests for database operations."""
import pytest
from datetime import datetime
from google_photos_organizer.database.models import GooglePhotoData, PhotoSource
from google_photos_organizer.database.db_manager import DatabaseManager


def create_test_photos(count: int) -> list[GooglePhotoData]:
    """Create test photo data."""
    current_time = datetime.now().isoformat()
    return [
        GooglePhotoData(
            id=f"test_photo_{i}",
            filename=f"test_{i}.jpg",
            normalized_filename=f"test_{i}.jpg",
            creation_time=current_time,
            mime_type="image/jpeg",
            width=1920,
            height=1080,
            path=f"test_{i}.jpg"
        )
        for i in range(count)
    ]


@pytest.mark.performance
def test_bulk_photo_insert(db_manager: DatabaseManager, benchmark):
    """Test the performance of bulk photo insertion."""
    # Initialize database tables
    db_manager.init_database()

    def bulk_insert():
        photos = create_test_photos(100)
        for photo in photos:
            db_manager.store_photo(photo, PhotoSource.GOOGLE)

    # Run the benchmark
    benchmark(bulk_insert)


@pytest.mark.performance
def test_photo_query_performance(db_manager: DatabaseManager, benchmark):
    """Test the performance of photo queries."""
    # Initialize database tables
    db_manager.init_database()

    # Setup test data
    photos = create_test_photos(100)
    for photo in photos:
        db_manager.store_photo(photo, PhotoSource.GOOGLE)

    def query_photos():
        return db_manager.search_photos("test", "test")

    # Run the benchmark
    result = benchmark(query_photos)
    assert len(result) == 100
