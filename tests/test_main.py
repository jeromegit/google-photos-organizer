"""Test module for main.py functionality."""
import pytest
from unittest.mock import patch
from google_photos_organizer.main import parse_arguments


def test_main_help(capsys):
    """Test that the main help message is displayed correctly."""
    with pytest.raises(SystemExit) as exc_info:
        with patch('sys.argv', ['script_name', '-h']):
            parse_arguments()
    
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    help_output = captured.out
    
    # Verify expected help content
    assert 'Google Photos Organizer' in help_output
    assert '--local-photos-dir' in help_output
    assert '--dry-run' in help_output
    assert 'positional arguments:' in help_output
    assert 'optional arguments:' in help_output
    assert '{scan-google,compare,search,scan-local,all}' in help_output
    assert 'scan-google' in help_output
    assert 'compare' in help_output
    assert 'search' in help_output
    assert 'scan-local' in help_output
    assert 'all' in help_output


@pytest.mark.parametrize('command', [
    'scan-google',
    'compare',
    'search',
    'scan-local',
    'all'
])
def test_subcommand_help(command, capsys):
    """Test that each subcommand's help message is displayed correctly."""
    with pytest.raises(SystemExit) as exc_info:
        with patch('sys.argv', ['script_name', command, '-h']):
            parse_arguments()
    
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    help_output = captured.out
    
    # Verify command-specific help content
    assert command in help_output
    
    # Verify command-specific arguments
    if command == 'scan-google':
        assert '--max-photos' in help_output
    elif command == 'compare':
        assert '--album-filter' in help_output
    elif command == 'search':
        assert 'pattern' in help_output
    elif command == 'all':
        assert '--max-photos' in help_output
