import os
import pickle
from typing import Dict, List, Set
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from collections import defaultdict
from urllib.parse import urlparse, unquote
import re
import sqlite3
import argparse

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/photoslibrary']

class GooglePhotosOrganizer:
    def __init__(self, source_dir: str):
        self.source_dir = source_dir
        self.service = None
        self.duplicates = defaultdict(list)
        self.conn = None

    def authenticate(self):
        """Authenticate with Google Photos API."""
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        self.service = build('photoslibrary', 'v1', credentials=creds, static_discovery=False)

    def get_album_title(self, path: str) -> str:
        """Convert directory path to album title using | as separator."""
        relative_path = os.path.relpath(path, self.source_dir)
        return relative_path.replace(os.sep, '|')

    def extract_filename(self, url: str) -> str:
        """Extract filename from Google Photos URL."""
        parsed = urlparse(url)
        path = unquote(parsed.path)
        # Try to find the original filename in the URL
        match = re.search(r'([^/]+)\.[^.]+$', path)
        return match.group(1) if match else path.split('/')[-1]

    def find_duplicates_in_google_photos(self) -> Dict[str, List[dict]]:
        """Find duplicate photos in Google Photos based on filename and metadata."""
        print("Fetching all photos from Google Photos...")
        duplicates = defaultdict(list)
        page_token = None
        
        while True:
            try:
                # Fetch media items
                request_body = {
                    'pageSize': 100,
                    'pageToken': page_token
                }
                response = self.service.mediaItems().list(**request_body).execute()
                
                if not response:
                    break
                    
                media_items = response.get('mediaItems', [])
                print(f"Found {len(media_items)} media items")
                for item in media_items:
                    filename = self.extract_filename(item['filename'])
                    # Store more metadata for better duplicate detection
                    item_info = {
                        'id': item['id'],
                        'filename': item['filename'],
                        'mimeType': item['mimeType'],
                        'mediaMetadata': item.get('mediaMetadata', {}),
                        'productUrl': item['productUrl']
                    }
                    duplicates[filename].append(item_info)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                    
            except HttpError as error:
                print(f"Error fetching media items: {error}")
                break
        
        # Filter out non-duplicates
        return {k: v for k, v in duplicates.items() if len(v) > 1}

    def list_albums_and_photos(self):
        """
        Retrieve all albums and their associated media items.
        
        Returns:
            dict: A dictionary where keys are album titles and values are lists of media items.
        """
        albums_dict = {}
        
        # List albums
        try:
            albums_request = self.service.albums().list(pageSize=50)
            albums_response = albums_request.execute()
            albums = albums_response.get('albums', [])
            
            # Iterate through each album
            for album in albums:
                album_id = album['id']
                album_title = album.get('title', 'Untitled Album')
                
                # Retrieve media items for this album
                media_items = []
                next_page_token = None
                
                while True:
                    # Request media items for the album
                    media_request = self.service.mediaItems().search(
                        body={'albumId': album_id, 'pageToken': next_page_token}
                    )
                    media_response = media_request.execute()
                    
                    # Add media items from this page
                    page_media_items = media_response.get('mediaItems', [])
                    media_items.extend(page_media_items)
                    
                    # Check for next page
                    next_page_token = media_response.get('nextPageToken')
                    if not next_page_token:
                        break
                
                # Store album and its media items
                albums_dict[album_title] = media_items
                
                print(f"Album: {album_title}, Total Media Items: {len(media_items)}")
        
        except Exception as e:
            print(f"Error retrieving albums: {e}")
        
        return albums_dict

    def create_albums(self):
        """Create albums in Google Photos based on directory structure."""
        for root, _, files in os.walk(self.source_dir):
            if files:  # Only create albums for directories containing files
                album_title = self.get_album_title(root)
                try:
                    # Check if album already exists
                    response = self.service.albums().list().execute()
                    albums = response.get('albums', [])
                    existing_album = next((album for album in albums 
                                        if album['title'] == album_title), None)
                    
                    if not existing_album:
                        # Create new album
                        album_body = {
                            'album': {'title': album_title}
                        }
                        self.service.albums().create(body=album_body).execute()
                        print(f"Created album: {album_title}")
                    else:
                        print(f"Album already exists: {album_title}")

                except HttpError as error:
                    print(f"Error creating album {album_title}: {error}")

    def init_database(self):
        """Initialize SQLite database with required tables."""
        self.conn = sqlite3.connect('photos.db')
        cursor = self.conn.cursor()

        # Create photos table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS photos (
                id TEXT PRIMARY KEY,
                filename TEXT,
                description TEXT,
                creation_time TEXT,
                mime_type TEXT,
                width INTEGER,
                height INTEGER,
                url TEXT
            )
        ''')

        # Create albums table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS albums (
                id TEXT PRIMARY KEY,
                title TEXT,
                creation_time TEXT,
                media_item_count INTEGER
            )
        ''')

        # Create album_photos relationship table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS album_photos (
                album_id TEXT,
                photo_id TEXT,
                FOREIGN KEY (album_id) REFERENCES albums(id),
                FOREIGN KEY (photo_id) REFERENCES photos(id),
                PRIMARY KEY (album_id, photo_id)
            )
        ''')

        self.conn.commit()

    def store_photos(self, max_photos=1000):
        """Store photos in SQLite database with progress information."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        photos_stored = 0
        next_page_token = None
        page_size = 100  # Fixed page size for all requests

        try:
            print("\nFetching photos...")
            while photos_stored < max_photos:
                try:
                    response = self.service.mediaItems().list(
                        pageSize=page_size,  # Use fixed page size
                        pageToken=next_page_token
                    ).execute()
                except Exception as api_error:
                    print(f"API Error: {api_error}")
                    return False
                
                media_items = response.get('mediaItems', [])
                if not media_items:
                    print("No more media items found")
                    break
                
                # Calculate how many items we can process from this batch
                items_remaining = max_photos - photos_stored
                items_to_process = min(len(media_items), items_remaining)
                
                for item in media_items[:items_to_process]:
                    try:
                        cursor.execute('''
                            INSERT OR REPLACE INTO photos 
                            (id, filename, description, creation_time, mime_type, width, height, url)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            item['id'],
                            item['filename'],
                            item.get('description', ''),
                            item.get('mediaMetadata', {}).get('creationTime', ''),
                            item.get('mimeType', ''),
                            int(item.get('mediaMetadata', {}).get('width', 0)),
                            int(item.get('mediaMetadata', {}).get('height', 0)),
                            item.get('productUrl', '')
                        ))
                    except sqlite3.Error as db_error:
                        print(f"Database Error on item {item.get('id', 'unknown')}: {db_error}")
                        continue
                    except Exception as item_error:
                        print(f"Error processing item {item.get('id', 'unknown')}: {item_error}")
                        print(f"Item data: {item}")
                        continue
                    
                    photos_stored += 1
                    if photos_stored % 100 == 0:
                        print(f"Photos progress: {photos_stored}/{max_photos}")
                        self.conn.commit()
                
                if photos_stored >= max_photos:
                    break
                    
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    print("No more pages available")
                    break
            
            self.conn.commit()
            print(f"Total photos stored: {photos_stored}")
            return True

        except Exception as e:
            print(f"Error storing photos: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
            return False

    def store_albums(self):
        """Store albums in SQLite database with progress information."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        try:
            print("\nFetching albums...")
            albums_request = self.service.albums().list(pageSize=50)
            albums_response = albums_request.execute()
            albums = albums_response.get('albums', [])
            
            for i, album in enumerate(albums, 1):
                cursor.execute('''
                    INSERT OR REPLACE INTO albums 
                    (id, title, creation_time, media_item_count)
                    VALUES (?, ?, ?, ?)
                ''', (
                    album['id'],
                    album.get('title', 'Untitled Album'),
                    album.get('creationTime', ''),
                    album.get('mediaItemsCount', 0)
                ))
                
                print(f"Albums progress: {i}/{len(albums)}")
                self.conn.commit()
            
            print(f"Total albums stored: {len(albums)}")
            return albums

        except Exception as e:
            print(f"Error storing albums: {e}")
            self.conn.rollback()
            return []

    def store_album_photos(self, albums):
        """Store album-photo relationships in SQLite database."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        try:
            print("\nFetching album photos...")
            for i, album in enumerate(albums, 1):
                next_page_token = None
                photos_in_album = 0
                
                while True:
                    media_request = self.service.mediaItems().search(
                        body={'albumId': album['id'], 'pageToken': next_page_token}
                    )
                    media_response = media_request.execute()
                    
                    media_items = media_response.get('mediaItems', [])
                    if not media_items:
                        break
                        
                    for media_item in media_items:
                        cursor.execute('''
                            INSERT OR REPLACE INTO album_photos 
                            (album_id, photo_id) VALUES (?, ?)
                        ''', (album['id'], media_item['id']))
                        photos_in_album += 1
                    
                    next_page_token = media_response.get('nextPageToken')
                    if not next_page_token:
                        break
                
                print(f"Album {i}/{len(albums)}: {album.get('title', 'Untitled')} - {photos_in_album} photos")
                self.conn.commit()
            
            return True

        except Exception as e:
            print(f"Error storing album photos: {e}")
            self.conn.rollback()
            return False

    def store_photos_and_albums(self, max_photos=1000):
        """Store photos and albums in SQLite database with progress information."""
        self.init_database()
        
        # Step 1: Store photos
        if not self.store_photos(max_photos):
            print("Failed to store photos")
            return False
            
        # Step 2: Store albums
        albums = self.store_albums()
        if not albums:
            print("Failed to store albums")
            return False
            
        # Step 3: Store album-photo relationships
        if not self.store_album_photos(albums):
            print("Failed to store album-photo relationships")
            return False
            
        print("\nSuccessfully stored all photos and albums in database")
        return True

    def print_album_contents(self):
        """Print all albums and their photos from the database."""
        cursor = self.conn.cursor()
        
        # Get all albums
        cursor.execute('''
            SELECT id, title FROM albums
        ''')
        albums = cursor.fetchall()
        
        for album_id, album_title in albums:
            print(f"\nAlbum: {album_title}")
            
            # Get photos in this album
            cursor.execute('''
                SELECT p.filename, p.creation_time, p.description
                FROM photos p
                JOIN album_photos ap ON p.id = ap.photo_id
                WHERE ap.album_id = ?
            ''', (album_id,))
            
            photos = cursor.fetchall()
            print(f"Total photos: {len(photos)}")
            
            for filename, creation_time, description in photos:
                desc_text = f" - {description}" if description else ""
                print(f"  - {filename} (Created: {creation_time}){desc_text}")

    def create_indices(self):
        """Create indices on the database tables for better query performance."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        try:
            print("\nCreating database indices...")
            
            # Indices for photos table
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_photos_filename 
                ON photos(filename)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_photos_creation_time 
                ON photos(creation_time)
            ''')
            
            # Indices for albums table
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_albums_title 
                ON albums(title)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_albums_creation_time 
                ON albums(creation_time)
            ''')
            
            # Indices for album_photos table
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_album_photos_album_id 
                ON album_photos(album_id)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_album_photos_photo_id 
                ON album_photos(photo_id)
            ''')
            
            self.conn.commit()
            print("Database indices created successfully")
            return True
            
        except sqlite3.Error as e:
            print(f"Error creating indices: {e}")
            self.conn.rollback()
            return False

def format_metadata(metadata: dict) -> str:
    """Format metadata for display."""
    if not metadata:
        return "No metadata available"
    
    result = []
    if 'creationTime' in metadata:
        result.append(f"Created: {metadata['creationTime']}")
    if 'width' in metadata and 'height' in metadata:
        result.append(f"Dimensions: {metadata['width']}x{metadata['height']}")
    if 'photo' in metadata:
        result.append(f"Camera Info: {metadata['photo'].get('cameraMake', 'Unknown')} {metadata['photo'].get('cameraModel', '')}")
    
    return " | ".join(result)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Google Photos Organizer')
    parser.add_argument('--source-dir', default='/Users/jerome/SMUGMUG_ALL',
                      help='Source directory for photos')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Store photos command
    photos_parser = subparsers.add_parser('photos', help='Store photos in database')
    photos_parser.add_argument('--max-photos', type=int, default=100000,
                           help='Maximum number of photos to store')
    
    # Store albums command
    albums_parser = subparsers.add_parser('albums', help='Store albums in database')
    
    # Store album photos command
    album_photos_parser = subparsers.add_parser('album-photos', 
                                             help='Store album-photo relationships')
    
    # Print contents command
    print_parser = subparsers.add_parser('print', help='Print album contents')
    
    # Create indices command
    indices_parser = subparsers.add_parser('create-indices', 
                                        help='Create database indices for better performance')
    
    # All command to run everything
    all_parser = subparsers.add_parser('all', help='Run all operations')
    all_parser.add_argument('--max-photos', type=int, default=100000,
                         help='Maximum number of photos to store')
    all_parser.add_argument('--create-indices', action='store_true',
                         help='Create database indices after storing data')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Initialize and authenticate
    organizer = GooglePhotosOrganizer(args.source_dir)
    print("Authenticating with Google Photos...")
    organizer.authenticate()
    
    if args.command == 'photos':
        print(f"\nStoring up to {args.max_photos} photos in database...")
        organizer.store_photos(args.max_photos)
    
    elif args.command == 'albums':
        print("\nStoring albums in database...")
        organizer.store_albums()
    
    elif args.command == 'album-photos':
        print("\nStoring album-photo relationships...")
        # First get albums
        albums = organizer.store_albums()
        if albums:
            organizer.store_album_photos(albums)
        else:
            print("No albums found or error storing albums")
    
    elif args.command == 'print':
        print("\nPrinting album contents from database...")
        organizer.print_album_contents()
    
    elif args.command == 'create-indices':
        print("\nCreating database indices...")
        organizer.create_indices()
    
    elif args.command == 'all':
        print("\nRunning all operations...")
        organizer.store_photos_and_albums(args.max_photos)
        if args.create_indices:
            organizer.create_indices()
        organizer.print_album_contents()
    
    else:
        parser = argparse.ArgumentParser(description='Google Photos Organizer')
        parser.print_help()

if __name__ == '__main__':
    main()
