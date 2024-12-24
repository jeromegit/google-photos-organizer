# Standard library imports
import argparse
import hashlib
import html
import json
import mimetypes
import os
import pickle
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any

# Third-party imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from PIL import Image
from tabulate import tabulate
from urllib.parse import urlparse, unquote

# Custom exceptions
class GooglePhotosError(Exception):
    """Base exception for Google Photos operations."""
    pass

class AuthenticationError(GooglePhotosError):
    """Raised when authentication fails."""
    pass

class ApiError(GooglePhotosError):
    """Raised when API calls fail."""
    pass

# Data models
@dataclass
class MediaItem:
    """Represents a media item in Google Photos."""
    id: str
    filename: str
    mime_type: str
    product_url: str
    metadata: Dict[str, Any]
    
@dataclass
class Album:
    """Represents an album in Google Photos."""
    id: str
    title: str
    media_items: List[MediaItem]

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/photoslibrary']

class GooglePhotosOrganizer:
    """
    A class to organize and manage Google Photos albums and media items.
    
    This class provides functionality to:
    - Authenticate with Google Photos API
    - Manage albums and media items
    - Find duplicates
    - Synchronize with local files
    - Store data in SQLite database
    
    Attributes:
        source_dir (str): Directory containing local photos
        service: Google Photos API service object
        duplicates (defaultdict): Dictionary tracking duplicate photos
        conn (sqlite3.Connection): SQLite database connection
    """
    
    def __init__(self, source_dir: str) -> None:
        """
        Initialize the GooglePhotosOrganizer.
        
        Args:
            source_dir: Path to directory containing local photos
        """
        self.source_dir = source_dir
        self.service = None
        self.duplicates: Dict[str, List[dict]] = defaultdict(list)
        self.conn: Optional[sqlite3.Connection] = None

    def authenticate(self) -> None:
        """
        Authenticate with Google Photos API.
        
        Raises:
            AuthenticationError: If authentication fails
        """
        try:
            creds = None
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    creds = pickle.load(token)
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    if not os.path.exists('credentials.json'):
                        raise AuthenticationError("credentials.json not found")
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)
                
                with open('token.pickle', 'wb') as token:
                    pickle.dump(creds, token)

            self.service = build('photoslibrary', 'v1', credentials=creds, static_discovery=False)
        except Exception as e:
            raise AuthenticationError(f"Failed to authenticate: {str(e)}")

    def get_album_title(self, path: str) -> str:
        """
        Convert directory path to album title using | as separator.
        
        Args:
            path: Directory path to convert
            
        Returns:
            Album title derived from path
        """
        relative_path = os.path.relpath(path, self.source_dir)
        return relative_path.replace(os.sep, '|')

    def extract_filename(self, url: str) -> str:
        """
        Extract filename from Google Photos URL.
        
        Args:
            url: Google Photos URL
            
        Returns:
            Extracted filename
        """
        parsed = urlparse(url)
        path = unquote(parsed.path)
        match = re.search(r'([^/]+)\.[^.]+$', path)
        return match.group(1) if match else path.split('/')[-1]

    def normalize_filename(self, filename: str) -> str:
        """
        Normalize filename by replacing non-alphanumeric chars with underscore.
        
        Args:
            filename: Original filename
            
        Returns:
            Normalized filename
        """
        name, ext = os.path.splitext(filename)
        name = re.sub(r'&[#\w]+;', '_', name)
        normalized = re.sub(r'[^a-zA-Z0-9]', '_', name)
        normalized = re.sub(r'_+', '_', normalized)
        normalized = normalized.strip('_')
        return normalized + ext.lower()

    def find_duplicates_in_google_photos(self) -> Dict[str, List[Dict[str, Any]]]:
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

    def list_albums_and_photos(self) -> Dict[str, List[MediaItem]]:
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

    def create_albums(self) -> None:
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

    def init_database(self) -> None:
        """
        Initialize SQLite database with necessary tables.
        
        Creates tables for photos, albums, and their relationships.
        
        Raises:
            sqlite3.Error: If database operations fail
        """
        try:
            self.conn = sqlite3.connect('photos.db')
            cursor = self.conn.cursor()
            
            # Create photos table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS google_photos (
                    id TEXT PRIMARY KEY,
                    filename TEXT,
                    mime_type TEXT,
                    product_url TEXT,
                    metadata TEXT
                )
            ''')
            
            # Create albums table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS google_albums (
                    id TEXT PRIMARY KEY,
                    title TEXT
                )
            ''')
            
            # Create album_photos relationship table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS google_album_photos (
                    album_id TEXT,
                    photo_id TEXT,
                    FOREIGN KEY (album_id) REFERENCES google_albums (id),
                    FOREIGN KEY (photo_id) REFERENCES google_photos (id),
                    PRIMARY KEY (album_id, photo_id)
                )
            ''')
            
            self.conn.commit()
        except sqlite3.Error as e:
            if self.conn:
                self.conn.rollback()
            raise ApiError(f"Database initialization failed: {str(e)}")

    def create_indices(self) -> None:
        """
        Create database indices for better query performance.
        
        Raises:
            sqlite3.Error: If index creation fails
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_photos_filename ON google_photos(filename)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_albums_title ON google_albums(title)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_album_photos_photo ON google_album_photos(photo_id)')
            self.conn.commit()
        except sqlite3.Error as e:
            if self.conn:
                self.conn.rollback()
            raise ApiError(f"Index creation failed: {str(e)}")

    def store_photos(self, max_photos: int = 100000) -> None:
        """
        Store photos in SQLite database.
        
        Args:
            max_photos: Maximum number of photos to store
            
        Raises:
            ApiError: If photo storage fails
        """
        try:
            cursor = self.conn.cursor()
            page_token = None
            photo_count = 0
            
            while photo_count < max_photos:
                request_body = {
                    'pageSize': 100,
                    'pageToken': page_token
                }
                
                try:
                    response = self.service.mediaItems().list(**request_body).execute()
                except HttpError as e:
                    raise ApiError(f"Failed to fetch media items: {str(e)}")
                
                if not response:
                    break
                    
                media_items = response.get('mediaItems', [])
                for item in media_items:
                    cursor.execute('''
                        INSERT OR REPLACE INTO google_photos (id, filename, mime_type, product_url, metadata)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        item['id'],
                        item['filename'],
                        item.get('mimeType', ''),
                        item.get('productUrl', ''),
                        json.dumps(item.get('mediaMetadata', {}))
                    ))
                    photo_count += 1
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                    
            self.conn.commit()
            print(f"Stored {photo_count} photos in database")
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            raise ApiError(f"Failed to store photos: {str(e)}")

    def store_albums(self) -> None:
        """
        Store albums in SQLite database.
        
        Raises:
            ApiError: If album storage fails
        """
        try:
            cursor = self.conn.cursor()
            albums_request = self.service.albums().list(pageSize=50)
            
            while albums_request:
                try:
                    albums_response = albums_request.execute()
                except HttpError as e:
                    raise ApiError(f"Failed to fetch albums: {str(e)}")
                    
                albums = albums_response.get('albums', [])
                for album in albums:
                    cursor.execute('''
                        INSERT OR REPLACE INTO google_albums (id, title)
                        VALUES (?, ?)
                    ''', (album['id'], album.get('title', 'Untitled Album')))
                
                albums_request = self.service.albums().list_next(
                    albums_request, albums_response)
                    
            self.conn.commit()
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            raise ApiError(f"Failed to store albums: {str(e)}")

    def store_album_photos(self, albums: Dict[str, List[MediaItem]]) -> None:
        """
        Store album-photo relationships in SQLite database.
        
        Args:
            albums: Dictionary mapping album IDs to lists of media items
            
        Raises:
            ApiError: If relationship storage fails
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM google_album_photos')  # Clear existing relationships
            
            for album_id, media_items in albums.items():
                for item in media_items:
                    cursor.execute('''
                        INSERT INTO google_album_photos (album_id, photo_id)
                        VALUES (?, ?)
                    ''', (album_id, item.id))
                    
            self.conn.commit()
            
        except sqlite3.Error as e:
            if self.conn:
                self.conn.rollback()
            raise ApiError(f"Failed to store album-photo relationships: {str(e)}")

    def store_photos_and_albums(self, max_photos: int = 100000) -> None:
        """
        Store both photos and albums in SQLite database.
        
        Args:
            max_photos: Maximum number of photos to store
            
        Raises:
            ApiError: If storage operations fail
        """
        self.init_database()
        self.create_indices()
        self.store_photos(max_photos)
        self.store_albums()
        albums = self.list_albums_and_photos()
        self.store_album_photos(albums)

    def print_album_contents(self) -> None:
        """
        Print all albums and their photos from the database.
        
        Raises:
            ApiError: If query fails
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT a.title, p.filename
                FROM google_albums a
                JOIN google_album_photos ap ON a.id = ap.album_id
                JOIN google_photos p ON ap.photo_id = p.id
                ORDER BY a.title, p.filename
            ''')
            
            current_album = None
            for title, filename in cursor.fetchall():
                if title != current_album:
                    if current_album is not None:
                        print()
                    print(f"\nAlbum: {title}")
                    current_album = title
                print(f"  - {filename}")
                
        except sqlite3.Error as e:
            raise ApiError(f"Failed to print album contents: {str(e)}")

    def init_local_tables(self) -> None:
        """Initialize SQLite tables for local files."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()

        # Create local photos table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS local_photos (
                id TEXT PRIMARY KEY,
                filename TEXT,
                normalized_filename TEXT,
                full_path TEXT,
                creation_time TEXT,
                mime_type TEXT,
                size INTEGER,
                width INTEGER,
                height INTEGER
            )
        ''')

        # Create local albums table (directories)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS local_albums (
                id TEXT PRIMARY KEY,
                title TEXT,
                full_path TEXT,
                creation_time TEXT,
                media_item_count INTEGER
            )
        ''')

        # Create local album_photos relationship table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS local_album_photos (
                album_id TEXT,
                photo_id TEXT,
                FOREIGN KEY (album_id) REFERENCES local_albums(id),
                FOREIGN KEY (photo_id) REFERENCES local_photos(id),
                PRIMARY KEY (album_id, photo_id)
            )
        ''')

        # Create indices
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_filename ON local_photos(filename)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_normalized_filename ON local_photos(normalized_filename)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_creation_time ON local_photos(creation_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_albums_title ON local_albums(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_albums_creation_time ON local_albums(creation_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_album_photos_album_id ON local_album_photos(album_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_album_photos_photo_id ON local_album_photos(photo_id)')

        self.conn.commit()

    def scan_local_directory(self) -> None:
        """Scan local directory and store information in database."""
        if not self.source_dir:
            print("No source directory specified")
            return False

        if not os.path.exists(self.source_dir):
            print(f"Source directory {self.source_dir} does not exist")
            return False

        self.init_local_tables()
        cursor = self.conn.cursor()
        photos_count = 0
        albums_count = 0

        try:
            print(f"\nScanning directory: {self.source_dir}")
            
            # Walk through directory
            for root, dirs, files in os.walk(self.source_dir):
                # Check if directory contains any media files
                media_files = [f for f in files if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.mp4', '.mov', '.avi'))]
                if not media_files:
                    continue
                
                # Create album (directory) entry
                rel_path = os.path.relpath(root, self.source_dir)
                if rel_path == '.':
                    album_title = os.path.basename(self.source_dir)
                else:
                    album_title = rel_path.replace(os.sep, ' | ')
                
                album_id = hashlib.md5(root.encode()).hexdigest()
                album_stat = os.stat(root)
                
                cursor.execute('''
                    INSERT OR REPLACE INTO local_albums 
                    (id, title, full_path, creation_time, media_item_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    album_id,
                    album_title,
                    root,
                    datetime.fromtimestamp(album_stat.st_mtime).isoformat(),
                    len(media_files)
                ))
                
                albums_count += 1
                if albums_count % 10 == 0:
                    print(f"Processed {albums_count} directories with media")
                
                # Process media files
                for file in media_files:
                    file_path = os.path.join(root, file)
                    file_stat = os.stat(file_path)
                    photo_id = hashlib.md5(file_path.encode()).hexdigest()
                    
                    # Try to get image dimensions for images
                    width = height = 0
                    if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
                        try:
                            with Image.open(file_path) as img:
                                width, height = img.size
                        except Exception as e:
                            print(f"Could not read dimensions for {file_path}: {e}")
                    
                    # Store photo/video information
                    cursor.execute('''
                        INSERT OR REPLACE INTO local_photos 
                        (id, filename, normalized_filename, full_path, creation_time, mime_type, size, width, height)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        photo_id,
                        file,
                        self.normalize_filename(file),
                        file_path,
                        datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                        mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                        file_stat.st_size,
                        width,
                        height
                    ))
                    
                    # Create album-photo relationship
                    cursor.execute('''
                        INSERT OR REPLACE INTO local_album_photos 
                        (album_id, photo_id) VALUES (?, ?)
                    ''', (album_id, photo_id))
                    
                    photos_count += 1
                    if photos_count % 100 == 0:
                        print(f"Processed {photos_count} media files")
                        self.conn.commit()
                
            self.conn.commit()
            print(f"\nCompleted scanning local directory:")
            print(f"- Processed {albums_count} directories with media")
            print(f"- Processed {photos_count} media files")
            return True
            
        except Exception as e:
            print(f"Error scanning directory: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
            return False

    def print_local_album_contents(self) -> None:
        """Print all local albums and their photos from the database."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        
        # Get all albums with media
        cursor.execute('''
            SELECT a.id, a.title, COUNT(DISTINCT ap.photo_id) as actual_count
            FROM local_albums a
            JOIN local_album_photos ap ON a.id = ap.album_id
            GROUP BY a.id, a.title
            HAVING actual_count > 0
            ORDER BY a.title
        ''')
        albums = cursor.fetchall()
        
        for album_id, album_title, count in albums:
            print(f"\nAlbum: {album_title}")
            print(f"Total media files: {count}")
            
            # Get photos in this album
            cursor.execute('''
                SELECT p.filename, p.creation_time, p.size, p.width, p.height, p.mime_type
                FROM local_photos p
                JOIN local_album_photos ap ON p.id = ap.photo_id
                WHERE ap.album_id = ?
                ORDER BY p.creation_time
            ''', (album_id,))
            
            photos = cursor.fetchall()
            for filename, creation_time, size, width, height, mime_type in photos:
                size_mb = size / (1024 * 1024)
                dimensions = f", Dimensions: {width}x{height}" if width and height else ""
                print(f"  - {filename} (Created: {creation_time}, Size: {size_mb:.1f}MB{dimensions}, Type: {mime_type})")

    def compare_with_google_photos(self, album_filter: Optional[str] = None) -> None:
        """Compare local albums and photos with Google Photos data."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        
        print("\nComparing local albums and photos with Google Photos...")
        
        # Get all local albums, with optional filter
        if album_filter:
            cursor.execute('''
                SELECT id, title, media_item_count
                FROM local_albums
                WHERE LOWER(title) LIKE LOWER(?)
                ORDER BY title
            ''', (f'%{album_filter}%',))
        else:
            cursor.execute('''
                SELECT id, title, media_item_count
                FROM local_albums
                ORDER BY title
            ''')
        local_albums = cursor.fetchall()
        
        for local_album_id, local_album_title, media_count in local_albums:
            # Check if album exists in Google Photos by title
            cursor.execute('''
                SELECT id, title
                FROM google_albums
                WHERE title = ?
            ''', (local_album_title,))
            google_album = cursor.fetchone()
            
            if not google_album:
                print(f"\nAlbum needs to be created: {local_album_title}")
                print(f"Checking {media_count} files in this album...")
                
                # Get all photos in this local album
                cursor.execute('''
                    SELECT lp.filename, lp.normalized_filename, lp.width, lp.height, lp.creation_time
                    FROM local_photos lp
                    JOIN local_album_photos lap ON lp.id = lap.photo_id
                    WHERE lap.album_id = ?
                ''', (local_album_id,))
                local_photos = cursor.fetchall()
                
                files_to_upload = []
                existing_files = []
                for filename, normalized_filename, width, height, creation_time in local_photos:
                    # Check if a similar photo exists in Google Photos
                    # We'll check by filename and dimensions for better accuracy
                    cursor.execute('''
                        SELECT id, filename
                        FROM google_photos
                        WHERE normalized_filename = ? AND width = ? AND height = ?
                    ''', (normalized_filename, width, height))
                    google_photo = cursor.fetchone()
                    
                    if not google_photo:
                        files_to_upload.append((filename, width, height))
                    else:
                        existing_files.append((filename, width, height))
                
                print(f"\nAnalyzing album: {local_album_title}")
                print(f"Total files in album: {len(local_photos)}")
                
                if files_to_upload:
                    print(f"\nFiles to upload ({len(files_to_upload)}):")
                    for filename, width, height in files_to_upload:
                        print(f"  - {filename} (Dimensions: {width}x{height})")
                
                if existing_files:
                    print(f"\nFiles already in Google Photos ({len(existing_files)}):")
                    for filename, width, height in existing_files:
                        print(f"  - {filename} (Dimensions: {width}x{height})")
                
                if not files_to_upload and not existing_files:
                    print("No files found in this album")
            else:
                # Album exists, check if all photos are in it
                cursor.execute('''
                    SELECT COUNT(DISTINCT lap.photo_id)
                    FROM local_album_photos lap
                    WHERE lap.album_id = ?
                ''', (local_album_id,))
                local_photo_count = cursor.fetchone()[0]
                
                cursor.execute('''
                    SELECT COUNT(DISTINCT ap.photo_id)
                    FROM google_album_photos ap
                    WHERE ap.album_id = ?
                ''', (google_album[0],))
                google_photo_count = cursor.fetchone()[0]
                
                if local_photo_count != google_photo_count:
                    print(f"\nAlbum '{local_album_title}' exists but has different number of photos:")
                    print(f"  - Local: {local_photo_count}")
                    print(f"  - Google Photos: {google_photo_count}")
                    
                    # Show which files are missing
                    cursor.execute('''
                        SELECT lp.filename, lp.width, lp.height
                        FROM local_photos lp
                        JOIN local_album_photos lap ON lp.id = lap.photo_id
                        WHERE lap.album_id = ?
                        AND NOT EXISTS (
                            SELECT 1
                            FROM google_photos p
                            JOIN google_album_photos ap ON p.id = ap.photo_id
                            WHERE ap.album_id = ?
                            AND p.filename = lp.filename
                            AND p.width = lp.width
                            AND p.height = lp.height
                        )
                    ''', (local_album_id, google_album[0]))
                    
                    missing_files = cursor.fetchall()
                    
                    # Get files that are already in the album
                    cursor.execute('''
                        SELECT lp.filename, lp.width, lp.height
                        FROM local_photos lp
                        JOIN google_photos p ON p.filename = lp.filename AND p.width = lp.width AND p.height = lp.height
                        JOIN google_album_photos ap ON p.id = ap.photo_id
                        WHERE lap.album_id = ? AND ap.album_id = ?
                    ''', (local_album_id, google_album[0]))
                    
                    existing_album_files = cursor.fetchall()
                    
                    print(f"\nAnalyzing album: {local_album_title}")
                    print(f"Local photos: {local_photo_count}")
                    print(f"Google Photos: {google_photo_count}")
                    
                    if missing_files:
                        print(f"\nFiles to add to the album ({len(missing_files)}):")
                        for filename, width, height in missing_files:
                            print(f"  - {filename} (Dimensions: {width}x{height})")
                    
                    if existing_album_files:
                        print(f"\nFiles already in the album ({len(existing_album_files)}):")
                        for filename, width, height in existing_album_files:
                            print(f"  - {filename} (Dimensions: {width}x{height})")

    def search_files(self, filename_pattern: str) -> List[Dict[str, Any]]:
        """Search for files in both local and Google Photos databases."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        rows = []
        
        normalized_pattern = self.normalize_filename(filename_pattern)
        
        # Search local photos
        cursor.execute('''
            SELECT 'Local' as source, filename, normalized_filename, width, height, creation_time,
                   (SELECT GROUP_CONCAT(a.title, ' | ')
                    FROM local_albums a
                    JOIN local_album_photos ap ON a.id = ap.album_id
                    WHERE ap.photo_id = p.id) as albums
            FROM local_photos p
            WHERE filename LIKE ? OR normalized_filename LIKE ?
            ORDER BY normalized_filename
        ''', (f'%{filename_pattern}%', f'%{normalized_pattern}%'))
        rows.extend(cursor.fetchall())
        
        # Search Google photos
        cursor.execute('''
            SELECT 'Google' as source, filename, normalized_filename, width, height, creation_time,
                   (SELECT GROUP_CONCAT(a.title, ' | ')
                    FROM google_albums a
                    JOIN google_album_photos ap ON a.id = ap.album_id
                    WHERE ap.photo_id = p.id) as albums
            FROM google_photos p
            WHERE filename LIKE ? OR normalized_filename LIKE ?
            ORDER BY normalized_filename
        ''', (f'%{filename_pattern}%', f'%{normalized_pattern}%'))
        rows.extend(cursor.fetchall())
        
        # Sort all rows by normalized filename
        rows.sort(key=lambda x: x[2])
        
        if rows:
            headers = ['Source', 'Filename', 'Normalized', 'Width', 'Height', 'Creation Time', 'Albums']
            print(f"\nFiles containing '{filename_pattern}':")
            print(tabulate(rows, headers=headers, tablefmt='grid'))
            print(f"\nTotal files found: {len(rows)}")
        else:
            print(f"No files found containing '{filename_pattern}'")

    def get_file_metadata(self, filepath: str) -> Dict[str, Any]:
        """
        Get metadata for a local file.
        
        Args:
            filepath: Path to the file
            
        Returns:
            Dictionary containing file metadata
        """
        file_stat = os.stat(filepath)
        metadata = {
            'creation_time': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
            'size': file_stat.st_size,
            'mime_type': mimetypes.guess_type(filepath)[0] or 'application/octet-stream'
        }
        return metadata

    def get_image_dimensions(self, filepath: str) -> Tuple[int, int]:
        """
        Get dimensions of an image file.
        
        Args:
            filepath: Path to the image file
            
        Returns:
            Tuple of (width, height)
        """
        try:
            with Image.open(filepath) as img:
                return img.size
        except Exception as e:
            print(f"Could not read dimensions for {filepath}: {e}")
            return (0, 0)

    def calculate_file_hash(self, filepath: str) -> str:
        """
        Calculate SHA-256 hash of a file.
        
        Args:
            filepath: Path to the file
            
        Returns:
            Hex string of file hash
        """
        hash = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash.update(chunk)
        return hash.hexdigest()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Google Photos Organizer')
    
    # Global arguments
    parser.add_argument('--local-photos-dir', type=str, default='.', 
                       help='Local directory containing photos to process (default: current directory)')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Initialize command
    init_parser = subparsers.add_parser('init', help='Initialize the database')
    init_parser.add_argument('--max-photos', type=int, default=100000, help='Maximum number of photos to process')

    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare local photos with Google Photos')
    compare_parser.add_argument('--album-filter', type=str, help='Filter albums by name (case-insensitive)')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search for files by filename')
    search_parser.add_argument('filename', type=str, help='Search for files containing this string')

    # Scan local command
    scan_parser = subparsers.add_parser('scan-local', help='Scan local directory and store in database')

    # All command
    all_parser = subparsers.add_parser('all', help='Initialize and compare')
    all_parser.add_argument('--max-photos', type=int, default=100000, help='Maximum number of photos to process')
    all_parser.add_argument('--album-filter', type=str, help='Filter albums by name (case-insensitive)')

    return parser.parse_args()

def main():
    args = parse_arguments()
    organizer = GooglePhotosOrganizer(args.local_photos_dir)

    if args.command == 'init':
        organizer.authenticate()
        organizer.store_photos_and_albums(args.max_photos)
    elif args.command == 'scan-local':
        organizer.scan_local_directory()
    elif args.command == 'compare':
        organizer.compare_with_google_photos(args.album_filter)
    elif args.command == 'search':
        organizer.search_files(args.filename)
    elif args.command == 'all':
        organizer.authenticate()
        organizer.store_photos_and_albums(args.max_photos)
        organizer.scan_local_directory()
        organizer.compare_with_google_photos(args.album_filter)
    else:
        parser = argparse.ArgumentParser(description='Google Photos Organizer')
        parser.print_help()

if __name__ == '__main__':
    main()
