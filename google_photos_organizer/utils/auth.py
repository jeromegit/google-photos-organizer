"""Authentication utilities for Google Photos API."""

import os
from typing import cast

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/photoslibrary']

def get_credentials(token_path: str, credentials_path: str) -> Credentials:
    """Get valid user credentials from storage.

    If there are no (valid) credentials available, let the user log in.

    Args:
        token_path: Path to token.json file
        credentials_path: Path to credentials.json file

    Returns:
        Valid credentials object

    Raises:
        FileNotFoundError: If credentials.json is not found
    """
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(
                    f"Missing credentials file at {credentials_path}"
                )

            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path,
                SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(token_path, 'w', encoding='utf-8') as token:
            token.write(creds.to_json())

    return cast(Credentials, creds)
