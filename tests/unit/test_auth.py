"""Unit tests for authentication utilities."""
import json
import pytest
from unittest.mock import MagicMock, patch, mock_open, create_autospec
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from google_photos_organizer.utils.auth import get_credentials, SCOPES, authenticate_google_photos

@pytest.fixture
def mock_token_data():
    """Create mock token data."""
    return {
        "token": "test_token",
        "refresh_token": "test_refresh_token",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "scopes": SCOPES
    }

@pytest.fixture
def mock_credentials_data():
    """Create mock credentials data."""
    return {
        "installed": {
            "client_id": "test_client_id",
            "project_id": "test-project",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": "test_client_secret",
            "redirect_uris": ["http://localhost"]
        }
    }

def test_get_credentials_from_token():
    """Test getting credentials from existing token file."""
    with patch('os.path.exists') as mock_exists, \
         patch.object(Credentials, 'from_authorized_user_file') as mock_from_file:
        
        mock_exists.return_value = True
        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_from_file.return_value = mock_creds
        
        creds = get_credentials("fake_token.json", "fake_credentials.json")
        
        assert creds is mock_creds
        mock_from_file.assert_called_once_with("fake_token.json", SCOPES)

def test_get_credentials_refresh():
    """Test refreshing expired credentials."""
    with patch('os.path.exists') as mock_exists, \
         patch.object(Credentials, 'from_authorized_user_file') as mock_from_file, \
         patch('builtins.open', mock_open()) as mock_file, \
         patch('google.auth.transport.requests.Request', return_value=create_autospec(Request)) as mock_request_class:
        
        mock_exists.return_value = True
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = "test_refresh_token"
        mock_creds.to_json.return_value = json.dumps({"token": "refreshed_token"})
        mock_from_file.return_value = mock_creds
        
        creds = get_credentials("fake_token.json", "fake_credentials.json")
        
        assert creds is mock_creds
        mock_creds.refresh.assert_called_once()
        mock_file().write.assert_called_once_with(json.dumps({"token": "refreshed_token"}))

def test_get_credentials_new_flow():
    """Test creating new credentials when no token exists."""
    with patch('os.path.exists') as mock_exists, \
         patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file') as mock_from_secrets, \
         patch('builtins.open', mock_open()) as mock_file:
        
        # First exists check for token file
        mock_exists.side_effect = [False, True]  # token.json doesn't exist, credentials.json does
        
        mock_flow = MagicMock()
        mock_creds = MagicMock()
        mock_creds.to_json.return_value = json.dumps({"token": "new_token"})
        mock_flow.run_local_server.return_value = mock_creds
        mock_from_secrets.return_value = mock_flow
        
        creds = get_credentials("fake_token.json", "fake_credentials.json")
        
        assert creds is mock_creds
        mock_from_secrets.assert_called_once_with("fake_credentials.json", SCOPES)
        mock_flow.run_local_server.assert_called_once_with(port=0)
        mock_file().write.assert_called_once_with(json.dumps({"token": "new_token"}))

def test_get_credentials_missing_credentials_file():
    """Test error when credentials file is missing."""
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = False
        with pytest.raises(FileNotFoundError):
            get_credentials("fake_token.json", "non_existent_credentials.json")

def test_authenticate_google_photos(mocker):
    """Test authenticating with Google Photos API."""
    # Mock the build function
    mock_service = mocker.Mock()
    mock_build = mocker.patch('google_photos_organizer.utils.auth.build', return_value=mock_service)
    
    # Mock get_credentials
    mock_creds = mocker.Mock()
    mocker.patch('google_photos_organizer.utils.auth.get_credentials', return_value=mock_creds)
    
    # Test successful authentication
    service = authenticate_google_photos()
    assert service == mock_service
    mock_build.assert_called_once_with('photoslibrary', 'v1', credentials=mock_creds, static_discovery=False)
    
    # Test authentication failure
    mock_build.side_effect = Exception("API Error")
    with pytest.raises(Exception) as exc_info:
        authenticate_google_photos()
    assert "Error authenticating with Google Photos" in str(exc_info.value)
