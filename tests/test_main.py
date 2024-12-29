"""Test module for main.py functionality."""

from unittest.mock import patch

import pytest

from google_photos_organizer.main import parse_arguments


def test_main_help(capsys):
    """Test that the main help message is displayed correctly."""
    with pytest.raises(SystemExit) as exc_info:
        with patch("sys.argv", ["script_name", "-h"]):
            parse_arguments()

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    help_output = captured.out

    # Verify expected help content
    assert "Google Photos Organizer" in help_output
    assert "--local-photos-dir" in help_output
    assert "--dry-run" in help_output
    assert "positional arguments:" in help_output
    assert "optional arguments:" in help_output
    
    # Verify all commands are present without enforcing order
    expected_commands = {
        "scan-google", "compare", "search",
        "scan-local", "match", "all"
    }
    for cmd in expected_commands:
        assert cmd in help_output


@pytest.mark.parametrize("command", ["scan-google", "compare", "search", "scan-local", "match", "all"])
def test_subcommand_help(command, capsys):
    """Test that each subcommand's help message is displayed correctly."""
    with pytest.raises(SystemExit) as exc_info:
        with patch("sys.argv", ["script_name", command, "-h"]):
            parse_arguments()

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    help_output = captured.out

    # Verify command-specific help content
    assert command in help_output

    # Verify command-specific arguments
    if command == "scan-google":
        assert "--max-photos" in help_output
    elif command == "compare":
        assert "--album-filter" in help_output
    elif command == "search":
        assert "pattern" in help_output
    elif command == "match":
        assert "--album-filter" in help_output
    elif command == "all":
        assert "-h, --help" in help_output  # Just check for basic help flag
