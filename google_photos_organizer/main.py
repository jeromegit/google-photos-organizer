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
import hashlib
import json
import mimetypes
from datetime import datetime
from PIL import Image
from tabulate import tabulate
import html

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

    def normalize_filename(self, filename):
        """Normalize filename by replacing non-alphanumeric chars with underscore and reducing consecutive underscores."""
        # Get file extension
        name, ext = os.path.splitext(filename)
        
        # Replace HTML entities like &#39; with underscore
        name = re.sub(r'&[#\w]+;', '_', name)
        
        # Replace non-alphanumeric chars with underscore
        normalized = re.sub(r'[^a-zA-Z0-9]', '_', name)
        
        # Replace multiple consecutive underscores with a single one
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        # Add back the extension
        return normalized + ext.lower()

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
        """Initialize SQLite database."""
        if self.conn:
            return

        self.conn = sqlite3.connect('photos.db')
        cursor = self.conn.cursor()

        # Create Google photos table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS google_photos (
                id TEXT PRIMARY KEY,
                filename TEXT,
                normalized_filename TEXT,
                description TEXT,
                creation_time TEXT,
                mime_type TEXT,
                size INTEGER,
                width INTEGER,
                height INTEGER,
                metadata TEXT
            )
        ''')

        # Create Google albums table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS google_albums (
                id TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                creation_time TEXT,
                media_item_count INTEGER
            )
        ''')

        # Create Google album_photos relationship table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS google_album_photos (
                album_id TEXT,
                photo_id TEXT,
                FOREIGN KEY (album_id) REFERENCES google_albums(id),
                FOREIGN KEY (photo_id) REFERENCES google_photos(id),
                PRIMARY KEY (album_id, photo_id)
            )
        ''')

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

        # Create local albums table
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

        self.conn.commit()

    def create_indices(self):
        """Create indices for better query performance."""
        if not self.conn:
            self.init_database()

        cursor = self.conn.cursor()
        print("\nCreating indices...")

        # Indices for Google Photos tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_photos_filename ON google_photos(filename)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_photos_normalized_filename ON google_photos(normalized_filename)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_photos_creation_time ON google_photos(creation_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_photos_mime_type ON google_photos(mime_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_photos_size ON google_photos(size)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_albums_title ON google_albums(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_albums_creation_time ON google_albums(creation_time)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_album_photos_album_id ON google_album_photos(album_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_google_album_photos_photo_id ON google_album_photos(photo_id)')

        # Indices for Local Photos tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_filename ON local_photos(filename)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_normalized_filename ON local_photos(normalized_filename)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_path ON local_photos(full_path)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_creation_time ON local_photos(creation_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_mime_type ON local_photos(mime_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_photos_size ON local_photos(size)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_albums_title ON local_albums(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_albums_path ON local_albums(full_path)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_album_photos_album_id ON local_album_photos(album_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_local_album_photos_photo_id ON local_album_photos(photo_id)')

        self.conn.commit()
        print("Indices created successfully")

    def store_photos(self, max_photos=100000):
        """Store photos in SQLite database."""
        if not self.conn:
            self.init_database()
            
        cursor = self.conn.cursor()
        stored_count = 0
        page_token = None
        
        try:
            while True:
                results = self.service.mediaItems().list(
                    pageSize=100,
                    pageToken=page_token
                ).execute()
                
                items = results.get('mediaItems', [])
                
                for item in items:
                    metadata = item.get('mediaMetadata', {})
                    creation_time = metadata.get('creationTime')
                    width = int(metadata.get('width', 0))
                    height = int(metadata.get('height', 0))
                    filename = item.get('filename')
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO google_photos 
                        (id, filename, normalized_filename, description, creation_time, mime_type, size, width, height, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        item['id'],
                        filename,
                        self.normalize_filename(filename),
                        item.get('description'),
                        creation_time,
                        item.get('mimeType'),
                        0,  # Size is not available in the API response
                        width,
                        height,
                        json.dumps(metadata)
                    ))
                    
                    stored_count += 1
                    if stored_count % 100 == 0:
                        print(f"Stored {stored_count} photos")
                        self.conn.commit()
                    
                    if stored_count >= max_photos:
                        break
                
                if stored_count >= max_photos:
                    break
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            self.conn.commit()
            print(f"Successfully stored {stored_count} photos")
            return True
            
        except Exception as e:
            print(f"Error storing photos: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
            return False

    def store_albums(self):
        """Store albums in SQLite database."""
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
                    INSERT OR REPLACE INTO google_albums 
                    (id, title, description, creation_time, media_item_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    album['id'],
                    album.get('title', 'Untitled Album'),
                    album.get('description', ''),
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
                            INSERT OR REPLACE INTO google_album_photos 
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

    def store_photos_and_albums(self, max_photos=100000):
        """Store photos and albums in SQLite database."""
        self.init_database()
        
        # Step 1: Store photos
        if self.store_photos(max_photos):
            # Step 2: Store albums and their photos
            albums = self.store_albums()
            if albums:
                self.store_album_photos(albums)
                # Step 3: Create indices after all data is loaded
                self.create_indices()
                return True
        return False

    def print_album_contents(self):
        """Print all albums and their photos from the database."""
        cursor = self.conn.cursor()
        
        # Get all albums
        cursor.execute('''
            SELECT id, title FROM google_albums
        ''')
        albums = cursor.fetchall()
        
        for album_id, album_title in albums:
            print(f"\nAlbum: {album_title}")
            
            # Get photos in this album
            cursor.execute('''
                SELECT p.filename, p.creation_time, p.description
                FROM google_photos p
                JOIN google_album_photos ap ON p.id = ap.photo_id
                WHERE ap.album_id = ?
            ''', (album_id,))
            
            photos = cursor.fetchall()
            print(f"Total photos: {len(photos)}")
            
            for filename, creation_time, description in photos:
                desc_text = f" - {description}" if description else ""
                print(f"  - {filename} (Created: {creation_time}){desc_text}")

    def init_local_tables(self):
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

    def scan_local_directory(self):
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

    def print_local_album_contents(self):
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

    def compare_with_google_photos(self, album_filter=None):
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

    def search_files(self, filename_pattern):
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
