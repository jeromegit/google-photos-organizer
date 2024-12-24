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
import argparse
import json
import mimetypes
from datetime import datetime
from PIL import Image
from tabulate import tabulate
import html
from google_photos_organizer.database.db_manager import DatabaseManager
from google_photos_organizer.utils import (
    normalize_filename,
    get_file_metadata,
    get_image_dimensions
)

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/photoslibrary']

class GooglePhotosOrganizer:
    def __init__(self, source_dir: str, dry_run: bool = False):
        self.source_dir = source_dir
        self.service = None
        self.duplicates = defaultdict(list)
        self.db = DatabaseManager('photos.db', dry_run=dry_run)

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

    def init_db(self):
        """Initialize the database tables."""
        if not self.db:
            self.db = DatabaseManager('photos.db')
        self.db.init_database()
        self.db.init_local_tables()

    def create_indices(self):
        """Create indices for better query performance."""
        if not self.db:
            self.init_db()

        self.db.create_indices()

    def store_photo_metadata(self, photo_id: str, filename: str, normalized_filename: str, 
                           mime_type: str, creation_time: str, width: int, height: int, 
                           product_url: str):
        """Store photo metadata in the database."""
        photo_data = self.db.PhotoData(
            id=photo_id,
            filename=filename,
            normalized_filename=normalized_filename,
            mime_type=mime_type,
            creation_time=creation_time,
            width=width,
            height=height,
            product_url=product_url
        )
        self.db.store_photo(photo_data)

    def store_album_metadata(self, album_id: str, title: str, creation_time: str):
        """Store album metadata in the database."""
        album_data = self.db.AlbumData(
            id=album_id,
            title=title,
            creation_time=creation_time
        )
        self.db.store_album(album_data)

    def store_album_photo_relation(self, album_id: str, photo_id: str):
        """Store album-photo relationship in the database."""
        self.db.store_album_photo(album_id, photo_id)

    def store_local_album_metadata(self, album_id: str, title: str, directory_path: str, 
                                 creation_time: str):
        """Store local album metadata in the database."""
        album_data = self.db.LocalAlbumData(
            id=album_id,
            title=title,
            full_path=directory_path,
            creation_time=creation_time
        )
        self.db.store_local_album(album_data)

    def store_local_photo_metadata(self, photo_id: str, filename: str, normalized_filename: str,
                                 file_path: str, creation_time: str, width: int, height: int,
                                 mime_type: str = None, size: int = 0):
        """Store local photo metadata in the database."""
        photo_data = self.db.LocalPhotoData(
            id=photo_id,
            filename=filename,
            normalized_filename=normalized_filename,
            full_path=file_path,
            creation_time=creation_time,
            mime_type=mime_type or 'application/octet-stream',
            size=size,
            width=width,
            height=height
        )
        self.db.store_local_photo(photo_data)

    def store_local_album_photo_relation(self, album_id: str, photo_id: str):
        """Store local album-photo relationship in the database."""
        self.db.store_local_album_photo(album_id, photo_id)

    def store_photos(self, max_photos=100000):
        """Store photos in SQLite database."""
        if not self.db:
            self.init_db()
            
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
                    
                    self.store_photo_metadata(
                        item['id'],
                        filename,
                        normalize_filename(filename),
                        item.get('mimeType'),
                        creation_time,
                        width,
                        height,
                        item.get('productUrl')
                    )
                    
                    stored_count += 1
                    if stored_count % 100 == 0:
                        print(f"Stored {stored_count} photos")
                    
                    if stored_count >= max_photos:
                        break
                
                if stored_count >= max_photos:
                    break
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            print(f"Successfully stored {stored_count} photos")
            return True
            
        except Exception as e:
            print(f"Error storing photos: {e}")
            import traceback
            traceback.print_exc()
            return False

    def store_albums(self):
        """Store albums in SQLite database."""
        if not self.db:
            self.init_db()
            
        try:
            print("\nFetching albums...")
            albums_request = self.service.albums().list(pageSize=50)
            albums_response = albums_request.execute()
            albums = albums_response.get('albums', [])
            
            for i, album in enumerate(albums, 1):
                self.store_album_metadata(
                    album['id'],
                    album.get('title', 'Untitled Album'),
                    album.get('creationTime', '')
                )
                
                print(f"Albums progress: {i}/{len(albums)}")
            
            print(f"Total albums stored: {len(albums)}")
            return albums

        except Exception as e:
            print(f"Error storing albums: {e}")
            return []

    def store_album_photos(self, albums):
        """Store album-photo relationships in SQLite database."""
        if not self.db:
            self.init_db()
            
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
                        self.store_album_photo_relation(album['id'], media_item['id'])
                        photos_in_album += 1
                    
                    next_page_token = media_response.get('nextPageToken')
                    if not next_page_token:
                        break
                
                print(f"Album {i}/{len(albums)}: {album.get('title', 'Untitled')} - {photos_in_album} photos")
            
            return True

        except Exception as e:
            print(f"Error storing album photos: {e}")
            return False

    def store_photos_and_albums(self, max_photos=100000):
        """Store photos and albums in SQLite database."""
        self.init_db()
        
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
        if not self.db:
            self.init_db()
            
        # Get all albums
        albums = self.db.get_albums()
        
        for album_id, album_title in albums:
            print(f"\nAlbum: {album_title}")
            
            # Get photos in this album
            photos = self.db.get_photos_in_album(album_id)
            print(f"Total photos: {len(photos)}")
            
            for filename, creation_time, description in photos:
                desc_text = f" - {description}" if description else ""
                print(f"  - {filename} (Created: {creation_time}){desc_text}")

    def scan_local_directory(self):
        """Scan local directory and store information in database."""
        if not self.source_dir:
            print("No source directory specified")
            return False

        if not os.path.exists(self.source_dir):
            print(f"Source directory {self.source_dir} does not exist")
            return False

        self.init_db()
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
                
                album_id = rel_path
                album_stat = os.stat(root)
                
                self.store_local_album_metadata(
                    album_id,
                    album_title,
                    root,
                    datetime.fromtimestamp(album_stat.st_mtime).isoformat()
                )
                
                albums_count += 1
                if albums_count % 10 == 0:
                    print(f"Processed {albums_count} directories with media")
                
                # Process media files
                for file in media_files:
                    file_path = os.path.join(root, file)
                    rel_file_path = os.path.relpath(file_path, self.source_dir)
                    file_stat = os.stat(file_path)
                    photo_id = rel_file_path
                    
                    # Try to get image dimensions for images
                    width = height = 0
                    if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
                        try:
                            width, height = get_image_dimensions(file_path)
                        except Exception as e:
                            print(f"Could not read dimensions for {file_path}: {e}")
                    
                    # Store photo/video information
                    self.store_local_photo_metadata(
                        photo_id,
                        file,
                        normalize_filename(file),
                        file_path,
                        datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                        width,
                        height,
                        mimetypes.guess_type(file)[0],
                        file_stat.st_size
                    )
                    
                    # Create album-photo relationship
                    self.store_local_album_photo_relation(album_id, photo_id)
                    
                    photos_count += 1
                    if photos_count % 100 == 0:
                        print(f"Processed {photos_count} media files")
            
            print(f"\nCompleted scanning local directory:")
            print(f"- Processed {albums_count} directories with media")
            print(f"- Processed {photos_count} media files")
            return True
            
        except Exception as e:
            print(f"Error scanning directory: {e}")
            import traceback
            traceback.print_exc()
            return False

    def print_local_album_contents(self):
        """Print all local albums and their photos from the database."""
        if not self.db:
            self.init_db()
            
        # Get all albums with media
        albums = self.db.get_local_albums()
        
        for album_id, album_title, count in albums:
            print(f"\nAlbum: {album_title}")
            print(f"Total media files: {count}")
            
            # Get photos in this album
            photos = self.db.get_photos_in_local_album(album_id)
            for filename, creation_time, size, width, height, mime_type in photos:
                size_mb = size / (1024 * 1024)
                dimensions = f", Dimensions: {width}x{height}" if width and height else ""
                print(f"  - {filename} (Created: {creation_time}, Size: {size_mb:.1f}MB{dimensions}, Type: {mime_type})")

    def compare_with_google_photos(self, album_filter=None):
        """Compare local albums and photos with Google Photos data."""
        if not self.db:
            self.init_db()
            
        print("\nComparing local albums and photos with Google Photos...")
        
        # Get all local albums, with optional filter
        if album_filter:
            albums = self.db.get_local_albums(album_filter)
        else:
            albums = self.db.get_local_albums()
        
        for local_album_id, local_album_title, media_count in albums:
            # Check if album exists in Google Photos by title
            album = self.db.get_album_by_title(local_album_title)
            
            if not album:
                print(f"\nAlbum needs to be created: {local_album_title}")
                print(f"Checking {media_count} files in this album...")
                
                # Get all photos in this local album
                photos = self.db.get_photos_in_local_album(local_album_id)
                
                files_to_upload = []
                existing_files = []
                for filename, creation_time, size, width, height, mime_type in photos:
                    # Check if a similar photo exists in Google Photos
                    # We'll check by filename and dimensions for better accuracy
                    photo = self.db.get_photo_by_filename_and_dimensions(filename, width, height)
                    
                    if not photo:
                        files_to_upload.append((filename, width, height))
                    else:
                        existing_files.append((filename, width, height))
                
                print(f"\nAnalyzing album: {local_album_title}")
                print(f"Total files in album: {len(photos)}")
                
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
                local_photo_count = self.db.get_photo_count_in_local_album(local_album_id)
                google_photo_count = self.db.get_photo_count_in_album(album[0])
                
                if local_photo_count != google_photo_count:
                    print(f"\nAlbum '{local_album_title}' exists but has different number of photos:")
                    print(f"  - Local: {local_photo_count}")
                    print(f"  - Google Photos: {google_photo_count}")
                    
                    # Show which files are missing
                    missing_files = self.db.get_missing_files_in_album(local_album_id, album[0])
                    
                    # Get files that are already in the album
                    existing_album_files = self.db.get_photos_in_album(album[0])
                    
                    print(f"\nAnalyzing album: {local_album_title}")
                    print(f"Local photos: {local_photo_count}")
                    print(f"Google Photos: {google_photo_count}")
                    
                    if missing_files:
                        print(f"\nFiles to add to the album ({len(missing_files)}):")
                        for filename, width, height in missing_files:
                            print(f"  - {filename} (Dimensions: {width}x{height})")
                    
                    if existing_album_files:
                        print(f"\nFiles already in the album ({len(existing_album_files)}):")
                        for filename, creation_time, description in existing_album_files:
                            desc_text = f" - {description}" if description else ""
                            print(f"  - {filename} (Created: {creation_time}){desc_text}")

    def search_files(self, filename_pattern):
        """Search for files in both local and Google Photos databases."""
        if not self.db:
            self.init_db()
            
        rows = []
        
        normalized_pattern = normalize_filename(filename_pattern)
        
        # Search local photos
        local_photos = self.db.search_local_photos(filename_pattern, normalized_pattern)
        rows.extend([(row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in local_photos])
        
        # Search Google photos
        google_photos = self.db.search_google_photos(filename_pattern, normalized_pattern)
        rows.extend([(row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in google_photos])
        
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
    parser.add_argument('--dry-run', action='store_true', help='Show database operations without executing them')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Scan Google Photos command
    scan_google_parser = subparsers.add_parser('scan-google', help='Scan Google Photos and store in database')
    scan_google_parser.add_argument('--max-photos', type=int, default=100000, help='Maximum number of photos to process')

    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare local photos with Google Photos')
    compare_parser.add_argument('--album-filter', type=str, help='Filter albums by name (case-insensitive)')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search for files by filename')
    search_parser.add_argument('filename', type=str, help='Search for files containing this string')

    # Scan local command
    scan_parser = subparsers.add_parser('scan-local', help='Scan local directory and store in database')

    # All command
    all_parser = subparsers.add_parser('all', help='Scan both Google Photos and local directory, then compare')
    all_parser.add_argument('--max-photos', type=int, default=100000, help='Maximum number of photos to process')
    all_parser.add_argument('--album-filter', type=str, help='Filter albums by name (case-insensitive)')

    return parser.parse_args()

def main():
    args = parse_arguments()
    organizer = GooglePhotosOrganizer(args.local_photos_dir, dry_run=args.dry_run)

    if args.command == 'scan-google':
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
