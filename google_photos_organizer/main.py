"""Main module for Google Photos Organizer."""
import os
import re
import argparse
import logging
import mimetypes
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional
from tabulate import tabulate
from urllib.parse import urlparse, unquote

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
    get_file_metadata
)
from google_photos_organizer.utils.auth import authenticate_google_photos
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

class GooglePhotosOrganizer:
    """Manages Google Photos organization and local photo scanning."""

    def __init__(self, local_photos_dir: str = '.', dry_run: bool = False):
        """Initialize the organizer."""
        self.local_photos_dir = local_photos_dir
        self.dry_run = dry_run
        self.service = None
        self.duplicates = defaultdict(list)
        self.db = DatabaseManager('photos.db', dry_run=dry_run)

    def authenticate(self):
        """Authenticate with Google Photos API."""
        try:
            self.service = authenticate_google_photos()
        except Exception as e:
            print(f"Error authenticating: {e}")
            return None

    def get_album_title(self, path: str) -> str:
        """Convert directory path to album title using | as separator."""
        relative_path = os.path.relpath(path, self.local_photos_dir)
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
                    }
                    duplicates[filename].append(item_info)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                    
            except Exception as e:
                print(f"Error fetching media items: {e}")
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

    def init_db(self):
        """Initialize the database tables."""
        if not self.db:
            self.db = DatabaseManager('photos.db')

    def create_indices(self):
        """Create indices for better query performance."""
        if not self.db:
            self.init_db()

        self.db.create_indices()

    def store_photo_metadata(self, photo_id: str, filename: str, normalized_filename: str, 
                           mime_type: str, creation_time: str, width: int, height: int):
        """Store photo metadata in the database."""
        # For Google Photos, we don't have a local path since the photos are in the cloud
        photo_data = GooglePhotoData(
            id=photo_id,
            filename=filename,
            normalized_filename=normalized_filename,
            mime_type=mime_type,
            creation_time=creation_time,
            width=width,
            height=height,
            path=""  # Empty string since Google Photos are stored in the cloud
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
                                 creation_time: str) -> None:
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
            path=directory_path,
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
            path=file_path,
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

    def store_photos(self, max_photos: Optional[int] = None) -> bool:
        """Store photos in SQLite database."""
        stored_count = 0
        page_token = None
        
        try:
            while True:
                results = self.service.mediaItems().list(
                    pageSize=100,
                    pageToken=page_token
                ).execute()
                
                items = results.get('mediaItems', [])
                if not items:
                    break
                
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
                        height
                    )
                    
                    stored_count += 1
                    if stored_count % 100 == 0:
                        print(f"Stored {stored_count} photos")
                    
                    if max_photos and stored_count >= max_photos:
                        print(f"\nReached maximum number of photos ({max_photos})")
                        return True
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            print(f"\nSuccessfully stored {stored_count} photos")
            return True
            
        except Exception as e:
            print(f"Error storing photos: {e}")
            import traceback
            traceback.print_exc()
            return False

    def store_albums(self) -> Optional[List[dict]]:
        """Store albums in SQLite database."""
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
            import traceback
            traceback.print_exc()
            return None

    def store_album_photos(self, albums: List[dict]) -> bool:
        """Store album-photo relationships in SQLite database."""
        try:
            print("\nFetching album photos...")
            
            for i, album in enumerate(albums, 1):
                album_id = album['id']
                
                # Get photos in this album
                request = self.service.mediaItems().search(
                    body={'albumId': album_id, 'pageSize': 100}
                )
                response = request.execute()
                media_items = response.get('mediaItems', [])
                
                for item in media_items:
                    self.store_album_photo_relation(album_id, item['id'])
                
                print(f"Album photos progress: {i}/{len(albums)}")
            
            print("Successfully stored all album-photo relationships")
            return True

        except Exception as e:
            print(f"Error storing album photos: {e}")
            import traceback
            traceback.print_exc()
            return False

    def store_photos_and_albums(self, max_photos: Optional[int] = None) -> None:
        """Store Google Photos photos and albums in database."""
        if not self.service:
            print("Google Photos service not initialized. Please authenticate first.")
            return

        # Initialize database for Google Photos only
        self.db.init_database(source=PhotoSource.GOOGLE)

        print("Scanning Google Photos...")
        
        # Store photos first
        if not self.store_photos(max_photos):
            print("Failed to store photos")
            return

        # Then store albums
        stored_albums = self.store_albums()
        if not stored_albums:
            print("Failed to store albums")
            return

        # Finally store album-photo relationships
        if not self.store_album_photos(stored_albums):
            print("Failed to store album-photo relationships")
            return

        print("\nDatabase summary:")
        print(f"Google Photos: {self.db.count_photos(PhotoSource.GOOGLE)}")

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
        if not self.local_photos_dir:
            logger.error("No source directory specified")
            return False

        if not os.path.exists(self.local_photos_dir):
            logger.error(f"Source directory {self.local_photos_dir} does not exist")
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
                path=file_path,
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
        """Scan local directory and store metadata in database."""
        if not self._validate_source_directory():
            return False

        # Initialize database for local photos only
        self.db.init_database(source=PhotoSource.LOCAL)

        print(f"\nScanning local directory: {self.local_photos_dir}")
        
        try:
            total_processed = 0

            # Process all files in source directory
            for root, _, files in os.walk(self.local_photos_dir):
                processed, failed = self._process_directory(root, files, self.local_photos_dir)
                total_processed += processed
                print(f"\rProcessed {total_processed} files...", end="", flush=True)

            print(f"\nFinished processing {total_processed} files")
            return True

        except Exception as e:
            print(f"Error scanning local directory: {e}")
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

    def search_files(self, filename_pattern: str) -> None:
        """Search for files in the database."""
        normalized_pattern = normalize_filename(filename_pattern)
        photos = self.db.search_photos(filename_pattern, normalized_pattern)
        rows = []
        for photo in photos:
            source = photo[0]
            filename = photo[1]
            normalized_name = photo[2]
            creation_time = photo[3]
            mime_type = photo[4]
            width = photo[5]
            height = photo[6]
            albums = photo[7] or ""
            
            rows.append([
                source,
                filename,
                normalized_name,
                creation_time,
                mime_type,
                f"{width}x{height}",
                albums
            ])
        
        if rows:
            print("\nSearch results:")
            print(tabulate(
                rows,
                headers=[
                    "Source", "Filename", "Normalized Name", "Creation Time",
                    "MIME Type", "Dimensions", "Albums"
                ],
                tablefmt="grid"
            ))
            print(f"\nTotal photos found: {len(rows)}")
        else:
            print(f"No photos found matching pattern: {filename_pattern}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Google Photos Organizer')
    
    # Global arguments
    parser.add_argument('--local-photos-dir', type=str, help='Local photos directory')
    parser.add_argument('--dry-run', action='store_true', help='Run without making changes')

    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Commands', required=True)

    # Scan Google Photos command
    scan_google_parser = subparsers.add_parser('scan-google', help='Scan Google Photos')
    scan_google_parser.add_argument('--max-photos', type=int, help='Maximum number of photos to process')

    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare photos')
    compare_parser.add_argument('--album-filter', type=str, help='Filter albums by name pattern')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search photos')
    search_parser.add_argument('pattern', type=str, help='Filename pattern to search for')

    # Scan local directory command
    scan_local_parser = subparsers.add_parser('scan-local', help='Scan local directory')

    # All command
    all_parser = subparsers.add_parser('all', help='Run all commands')
    all_parser.add_argument('--max-photos', type=int, help='Maximum number of photos to process')

    return parser.parse_args()

def main():
    args = parse_arguments()
    organizer = GooglePhotosOrganizer(args.local_photos_dir, dry_run=args.dry_run)

    if args.command == 'scan-google':
        organizer.authenticate()
        organizer.store_photos_and_albums(max_photos=args.max_photos)
    elif args.command == 'scan-local':
        organizer.scan_local_directory()
    elif args.command == 'compare':
        organizer.compare_with_google_photos(album_filter=args.album_filter)
    elif args.command == 'search':
        organizer.search_files(args.pattern)
    elif args.command == 'all':
        organizer.authenticate()
        organizer.store_photos_and_albums(max_photos=args.max_photos)
        organizer.scan_local_directory()
        organizer.compare_with_google_photos()
    else:
        parser = argparse.ArgumentParser(description='Google Photos Organizer')
        parser.print_help()

if __name__ == '__main__':
    main()
