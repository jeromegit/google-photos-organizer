from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Define the scopes required
SCOPES = [
    "https://www.googleapis.com/auth/photoslibrary",
    "https://www.googleapis.com/auth/photoslibrary.readonly",
]

# Run the authorization flow
flow = InstalledAppFlow.from_client_secrets_file(
    "/Users/jerome/projects/google-photos-organizer/client_secret.json", SCOPES
)
creds = flow.run_local_server(port=0)

# Save the credentials for reuse
with open("/Users/jerome/projects/google-photos-organizer/credentials.json", "w") as token:
    token.write(creds.to_json())

# Use the credentials
service = build("photoslibrary", "v1", credentials=creds, static_discovery=False)

# Example API call
albums = service.albums().list(pageSize=10).execute()
print(albums)
