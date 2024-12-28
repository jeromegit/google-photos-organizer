"""Main module for Google Photos Organizer."""
import os
import pickle
import re
import argparse
import logging
import mimetypes
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from tabulate import tabulate

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google_photos_organizer.database.models import (
    GoogleAlbumData,
    GooglePhotoData,
    LocalAlbumData,
    LocalPhotoData,
    PhotoSource
)
from google_photos_organizer.database.db_manager import DatabaseManager
from google_photos_organizer.utils.file_utils import (
    get_image_dimensions,
    normalize_filename,
    get_file_metadata,
    is_media_file
)

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/photoslibrary']

class GooglePhotosOrganizer:
    """Manages Google Photos organization and local photo scanning."""

    def __init__(self, source_dir: str, dry_run: bool = False):
        """Initialize the organizer.

        Args:
            source_dir: Directory to scan for photos
            dry_run: If True, show operations without executing them
        """
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
                    'client_secret.json', SCOPES)
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

    def list_albums_and_photos(self) -> Dict[str, List[Dict]]:
        """Retrieve all albums and their associated media items."""
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

    def create_indices(self):
        """Create indices for better query performance."""
        if not self.db:
            self.init_db()

        self.db.create_indices()

    def store_photo_metadata(self, photo_id: str, filename: str, normalized_filename: str, 
                           mime_type: str, creation_time: str, width: int, height: int, 
                           product_url: str = None):
        """Store photo metadata in the database."""
        photo_data = GooglePhotoData(
            id=photo_id,
            filename=filename,
            normalized_filename=normalized_filename,
            mime_type=mime_type,
            creation_time=creation_time,
            width=width,
            height=height,
            product_url=product_url
        )
        self.db.store_photo(photo_data, PhotoSource.GOOGLE)

    def store_album_metadata(self, album_id: str, title: str, creation_time: str):
        """Store album metadata in the database."""
        album_data = GoogleAlbumData(
            id=album_id,
            title=title,
            creation_time=creation_time
        )
        self.db.store_album(album_data, PhotoSource.GOOGLE)

    def store_album_photo_relation(self, album_id: str, photo_id: str):
        """Store album-photo relationship in the database."""
        self.db.store_album_photo(album_id, photo_id, PhotoSource.GOOGLE)

    def store_local_album_metadata(self, album_id: str, title: str, directory_path: str, 
                                 creation_time: str):
        """Store local album metadata in the database.

        Args:
            album_id: Unique identifier for the album
            title: Album title
            directory_path: Path to the album directory
            creation_time: Album creation/modification time
        """
        album_data = LocalAlbumData(
            id=album_id,
            title=title,
            full_path=directory_path,
            creation_time=creation_time
        )
        self.db.store_album(album_data, PhotoSource.LOCAL)

    def store_local_photo_metadata(self, photo_id: str, filename: str, normalized_filename: str,
                                 file_path: str, creation_time: str, width: int, height: int,
                                 mime_type: str = None, size: int = 0):
        """Store local photo metadata in the database."""
        photo_data = LocalPhotoData(
            id=photo_id,
            filename=filename,
            normalized_filename=normalized_filename,
            full_path=file_path,
            creation_time=creation_time,
            mime_type=mime_type,
            width=width,
            height=height,
            size=size
        )
        self.db.store_photo(photo_data, PhotoSource.LOCAL)

    def store_local_album_photo_relation(self, album_id: str, photo_id: str):
        """Store local album-photo relationship in the database."""
        self.db.store_album_photo(album_id, photo_id, PhotoSource.LOCAL)

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
                    print(f"Fetching photos for album {album['id']} ({album.get('title', 'Untitled')})")
                    media_request = self.service.mediaItems().search(
                        body={'albumId': album['id'], 'pageToken': next_page_token}
                    )
                    media_response = media_request.execute()
                    
                    media_items = media_response.get('mediaItems', [])
                    if not media_items:
                        print(f"No media items found in album {album.get('title', 'Untitled')}")
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
            import traceback
            traceback.print_exc()
            return False

    def store_photos_and_albums(self, max_photos=100000) -> None:
        """Store photos and albums in SQLite database."""
        if not self.service:
            self.authenticate()

        if not self.db:
            self.init_db()

        # Store photos first
        stored_photos = self.store_photos(max_photos)

        # Then store albums
        stored_albums = self.store_albums()

        # Finally store album-photo relationships
        if stored_albums:
            self.store_album_photos(stored_albums)

        # Create indices for better query performance
        self.db.create_indices(source=PhotoSource.GOOGLE)

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

    def _validate_source_directory(self) -> bool:
        """
        Validate that the source directory exists and is accessible.

        Returns:
            bool: True if directory is valid, False otherwise
        """
        if not self.source_dir:
            logger.error("No source directory specified")
            return False

        if not os.path.exists(self.source_dir):
            logger.error(f"Source directory {self.source_dir} does not exist")
            return False

        return True

    def _process_media_file(
        self, 
        file_path: str, 
        album_id: str,
        source_dir: str
    ) -> bool:
        """
        Process a single media file and store its metadata.

        Args:
            file_path: Full path to the media file
            album_id: ID of the album containing this file
            source_dir: Root source directory for relative path calculation

        Returns:
            bool: True if processing succeeded, False otherwise
        """
        try:
            file = os.path.basename(file_path)
            rel_file_path = os.path.relpath(file_path, source_dir)
            file_stat = os.stat(file_path)
            photo_id = rel_file_path
            
            width = height = 0
            if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
                try:
                    width, height = get_image_dimensions(file_path)
                except Exception as e:
                    logger.warning(f"Failed to get dimensions for {file_path}: {e}")

            metadata = get_file_metadata(file_path)
            if metadata:
                width = metadata.get('width', width)
                height = metadata.get('height', height)
                mime_type = metadata.get('mime_type', mimetypes.guess_type(file)[0])
            else:
                mime_type = mimetypes.guess_type(file)[0]

            photo_data = LocalPhotoData(
                id=photo_id,
                filename=file,
                normalized_filename=normalize_filename(file),
                full_path=file_path,
                creation_time=datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                mime_type=mime_type,
                width=width,
                height=height,
                size=file_stat.st_size
            )
            
            self.db.store_photo(photo_data, PhotoSource.LOCAL)
            self.db.store_album_photo(album_id, photo_id, PhotoSource.LOCAL)
            return True
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return False

    def _process_directory(
        self, 
        root: str, 
        files: list[str],
        source_dir: str
    ) -> tuple[int, int]:
        """
        Process a directory and its media files.

        Args:
            root: Directory path being processed
            files: List of files in the directory
            source_dir: Root source directory for relative path calculation

        Returns:
            tuple[int, int]: Count of (processed_files, failed_files)
        """
        media_files = [
            f for f in files 
            if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.mp4', '.mov', '.avi'))
        ]
        if not media_files:
            return 0, 0

        # Create album entry
        rel_path = os.path.relpath(root, source_dir)
        album_title = os.path.basename(source_dir) if rel_path == '.' else rel_path.replace(os.sep, ' | ')
        album_id = rel_path
        album_stat = os.stat(root)

        self.store_local_album_metadata(
            album_id=album_id,
            title=album_title,
            directory_path=root,
            creation_time=datetime.fromtimestamp(album_stat.st_mtime).isoformat()
        )

        processed_files = failed_files = 0
        for file in media_files:
            file_path = os.path.join(root, file)
            if self._process_media_file(file_path, album_id, source_dir):
                processed_files += 1
            else:
                failed_files += 1

        return processed_files, failed_files

    def scan_local_directory(self) -> bool:
        """
        Scan local directory and store media information in database.
        
        This method walks through the source directory, identifying media files
        and organizing them into albums based on their directory structure.
        
        Returns:
            bool: True if scan completed successfully, False otherwise
        
        Raises:
            OSError: If there are filesystem-related errors
            Exception: For other unexpected errors
        """
        if not self._validate_source_directory():
            return False

        if not self.db:
            self.init_db()

        try:
            total_processed = 0
            total_failed = 0

            for root, _, files in os.walk(self.source_dir):
                processed, failed = self._process_directory(root, files, self.source_dir)
                total_processed += processed
                total_failed += failed

            print(f"\nProcessed {total_processed} files")
            if total_failed > 0:
                print(f"Failed to process {total_failed} files")

            # Create indices for better query performance
            self.db.create_indices(source=PhotoSource.LOCAL)

            return True

        except Exception as e:
            print(f"Error scanning directory: {e}")
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
        
        # Search both local and Google photos in one query
        photos = self.db.search_photos(filename_pattern, normalized_pattern)
        rows.extend([(row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in photos])
        
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
