"""Configuration for unit tests."""
import pytest
import logging

@pytest.fixture(autouse=True)
def setup_logging():
    """Configure logging for tests."""
    logging.basicConfig(level=logging.DEBUG)
    yield
