"""Configuration for unit tests."""

import logging

import pytest


@pytest.fixture(autouse=True)
def setup_logging():
    """Configure logging for tests."""
    logging.basicConfig(level=logging.DEBUG)
    yield
