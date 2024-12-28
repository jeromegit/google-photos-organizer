"""Test configuration for pytest."""

import os
import sys
import pytest
from pathlib import Path
from typing import Generator

from google_photos_organizer.database.db_manager import DatabaseManager

# Add the project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture(scope="session")
def test_db_path(tmp_path_factory) -> Path:
    """Create a temporary database for testing."""
    db_dir = tmp_path_factory.mktemp("test_db")
    return db_dir / "test.db"

@pytest.fixture(scope="function")
def db_manager(test_db_path: Path) -> Generator[DatabaseManager, None, None]:
    """Create a test database manager."""
    manager = DatabaseManager(str(test_db_path))
    manager.init_database()
    yield manager
    # Cleanup
    if test_db_path.exists():
        os.unlink(test_db_path)
