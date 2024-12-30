"""Main module for Google Photos Organizer."""

import argparse
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import unquote, urlparse

from googleapiclient.discovery import Resource
from tabulate import tabulate

from google_photos_organizer.database.db_manager import DatabaseManager
from google_photos_organizer.database.models import (
    GoogleAlbumData,
    GooglePhotoData,
    LocalAlbumData,
    LocalPhotoData,
    PhotoSource,
)
from google_photos_organizer.utils.auth import authenticate_google_photos
from google_photos_organizer.utils.file_utils import (
    get_file_metadata,
    get_image_dimensions,
    is_media_file,
    normalize_filename,
)

logger = logging.getLogger(__name__)


class GooglePhotosOrganizer:
    """Manages Google Photos organization and local photo scanning."""

    def __init__(self, local_photos_dir: str = ".", dry_run: bool = False):
        """Initialize the organizer."""
        self.local_photos_dir = local_photos_dir
        self.dry_run = dry_run
        self.service: Optional[Resource] = None
        self.db = DatabaseManager("photos.db", dry_run=dry_run)

    def authenticate(self) -> None:
        """Authenticate with Google Photos API."""
        try:
            self.service = authenticate_google_photos()
            if not isinstance(self.service, Resource):
                raise TypeError("Failed to initialize Google Photos API service")
        except (ValueError, IOError) as e:
            logger.error("Authentication failed: %s", str(e))
            raise

    def get_album_title(self, path: str) -> str:
        """Convert directory path to album title using | as separator."""
        relative_path = os.path.relpath(path)
        return relative_path.replace(os.sep, "|")

    def extract_filename(self, url: str) -> str:
        """Extract filename from Google Photos URL."""
        path = unquote(urlparse(url).path)
        match = re.search(r"([^/]+)\.[^.]+$", path)
        return match.group(1) if match else path.split("/")[-1]

    def init_db(self):
        """Initialize the database tables."""
        if not self.db:
            self.db = DatabaseManager("photos.db")

    def create_indices(self):
        """Create indices for better query performance."""
        if not self.db:
            self.init_db()

        self.db.create_indices()

    def store_photo_metadata(
        self, photo_data: Union[GooglePhotoData, LocalPhotoData], source: PhotoSource
    ) -> None:
        """Store photo metadata in the database.

        Args:
            photo_data: Photo data object (GooglePhotoData or LocalPhotoData)
            source: Source of the photo (GOOGLE or LOCAL)
        """
        self.db.store_photo(photo_data, source)

    def store_local_photo_metadata(self, photo_data: LocalPhotoData) -> None:
        """Store local photo metadata in the database.

        Args:
            photo_data: Local photo data object
        """
        self.store_photo_metadata(photo_data, PhotoSource.LOCAL)

    def store_google_photo_metadata(self, photo_data: GooglePhotoData) -> None:
        """Store Google photo metadata in the database.

        Args:
            photo_data: Google photo data object
        """
        self.store_photo_metadata(photo_data, PhotoSource.GOOGLE)

    def store_album_metadata(self, album_data: GoogleAlbumData) -> None:
        """Store album metadata in the database."""
        self.db.store_album(album_data, PhotoSource.GOOGLE)

    def store_album_photo_relation(self, album_id: str, photo_id: str):
        """Store album-photo relationship in the database."""
        self.db.store_album_photo(album_id, photo_id, PhotoSource.GOOGLE)

    def store_local_album_metadata(
        self, album_id: str, album_title: str, path: str, album_time: str
    ) -> None:
        """Store local album metadata in the database."""
        self.db.store_album(
            LocalAlbumData(id=album_id, title=album_title, path=path, creation_time=album_time),
            PhotoSource.LOCAL,
        )

    def store_local_album_photo_relation(self, album_id: str, photo_id: str):
        """Store local album-photo relationship in the database."""
        self.db.store_album_photo(album_id, photo_id, PhotoSource.LOCAL)

    def store_photos(self, max_photos: Optional[int] = None) -> bool:
        """Store photos in SQLite database."""
        if not self.service:
            print("Not authenticated with Google Photos")
            return False

        print("Scanning Google Photos...")
        stored_count = 0
        page_token = None

        try:
            while True:
                results = (
                    self.service.mediaItems().list(pageSize=100, pageToken=page_token).execute()
                )
                items = results.get("mediaItems", [])

                if not items:
                    break

                for item in items:
                    metadata = item.get("mediaMetadata", {})
                    creation_time = metadata.get("creationTime")
                    width = int(metadata.get("width", 0))
                    height = int(metadata.get("height", 0))
                    filename = item.get("filename")

                    self.store_google_photo_metadata(
                        GooglePhotoData(
                            id=item["id"],
                            filename=filename,
                            normalized_filename=normalize_filename(filename),
                            creation_time=creation_time,
                            width=width,
                            height=height,
                            mime_type=item.get("mimeType"),
                            path=item["id"],
                        )
                    )

                    stored_count += 1
                    if stored_count % 100 == 0:
                        print(f"Stored {stored_count} photos")

                    if max_photos and stored_count >= max_photos:
                        print(f"\nReached maximum number of photos ({max_photos})")
                        return True

                page_token = results.get("nextPageToken")
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
            albums = albums_response.get("albums", [])

            for i, album in enumerate(albums, 1):
                self.store_album_metadata(
                    GoogleAlbumData(
                        id=album["id"],
                        title=album.get("title", "Untitled Album"),
                        creation_time=album.get("creationTime", ""),
                    )
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
                album_id = album["id"]

                # Get photos in this album
                request = self.service.mediaItems().search(
                    body={"albumId": album_id, "pageToken": None, "pageSize": 100}
                )
                response = request.execute()

                # Add media items from this page
                page_media_items = response.get("mediaItems", [])
                for item in page_media_items:
                    self.store_album_photo_relation(album_id, item["id"])

                # Check for next page
                next_page_token = response.get("nextPageToken")
                while next_page_token:
                    request = self.service.mediaItems().search(
                        body={"albumId": album_id, "pageToken": next_page_token, "pageSize": 100}
                    )
                    response = request.execute()
                    page_media_items = response.get("mediaItems", [])
                    for item in page_media_items:
                        self.store_album_photo_relation(album_id, item["id"])
                    next_page_token = response.get("nextPageToken")

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
            for filename, creation_time, width, height, mime_type in photos:
                dimensions = f", Dimensions: {width}x{height}" if width and height else ""
                print(
                    f"  - {filename} (Created: {creation_time}" f"{dimensions}, Type: {mime_type})"
                )

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

            rows.append(
                [
                    source,
                    filename,
                    normalized_name,
                    creation_time,
                    mime_type,
                    f"{width}x{height}",
                    albums,
                ]
            )

        if rows:
            print("\nSearch results:")
            print(
                tabulate(
                    rows,
                    headers=[
                        "Source",
                        "Filename",
                        "Normalized Name",
                        "Creation Time",
                        "MIME Type",
                        "Dimensions",
                        "Albums",
                    ],
                    tablefmt="psql",
                )
            )
            print(f"\nTotal photos found: {len(rows)}")
        else:
            print(f"No photos found matching pattern: {filename_pattern}")

    def find_matching_photos(self, album_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Find matching photos between local and Google Photos based on filename and dimensions.

        All local photos will be included in the results, even if there is no match in Google Photos.
        For unmatched photos, the Google Photos fields will be left empty.
        """
        results = []

        # Get local photos, filtered by album if specified
        local_photos = self.db.get_local_photos(album_filter)
        total_photos = len(local_photos)
        print(f"\nSearching for matches among {total_photos} local photos...")

        for idx, local_photo in enumerate(local_photos, 1):
            if idx % 100 == 0:
                print(f"Processed {idx}/{total_photos} photos...")

            result = {
                "album_title": local_photo["album_title"],
                "filename": local_photo["filename"],
                "normalized_filename": local_photo["normalized_filename"],
                "dimensions": f"{local_photo['width']}x{local_photo['height']}",
                "google_filename": "",  # Default empty for unmatched photos
                "google_id": "",  # Default empty for unmatched photos
                "google_album": "",  # Default empty for unmatched photos
            }

            # Find Google photos with matching normalized filename
            google_matches = self.db.find_google_photos_by_filename(
                local_photo["normalized_filename"]
            )

            if google_matches:
                # If multiple matches, try to match by dimensions
                if len(google_matches) > 1:
                    print(
                        f"Multiple matches found, checking dimensions {local_photo['width']}x{local_photo['height']}"
                    )
                    exact_matches = [
                        gp
                        for gp in google_matches
                        if gp["width"] == local_photo["width"]
                        and gp["height"] == local_photo["height"]
                    ]
                    if exact_matches:
                        google_matches = exact_matches

                # Take the first match (or only match)
                google_photo = google_matches[0]
                result["google_filename"] = google_photo["filename"]
                result["google_id"] = google_photo["id"]
                result["google_album"] = google_photo["album_title"]

            results.append(result)

        return results

    def print_matching_photos(self, album_filter: Optional[str] = None) -> None:
        """Find and print matching photos between local and Google Photos.

        Args:
            album_filter: Optional album title to filter local photos by
        """
        results = self.find_matching_photos(album_filter)
        if results:
            print("\nMatching photos:")
            print(tabulate(results, headers="keys", tablefmt="psql"))
            print(f"\nFound {len(results)} matches")
        else:
            print("No matches found")

    def scan_local_directory(self) -> None:
        """Scan local directory and store information in database."""
        if not os.path.exists(self.local_photos_dir):
            logging.error("Local photos directory does not exist: %s", self.local_photos_dir)
            return

        # Initialize only local tables
        self.db.init_database(source=PhotoSource.LOCAL)
        print(f"\nScanning local directory: {self.local_photos_dir}")

        # Track stats
        total_files = 0
        total_albums = 0
        current_album_files = 0

        for root, dirs, files in os.walk(self.local_photos_dir):
            # Remove hidden directories
            dirs[:] = [d for d in dirs if not d.startswith(".")]

            # Create album for this directory if it has media files
            media_files = [f for f in files if not f.startswith(".") and is_media_file(f)]

            if not media_files:
                continue

            album_path = os.path.relpath(root, self.local_photos_dir)
            album_title = self.get_album_title(album_path)
            album_id = album_path
            album_time = datetime.fromtimestamp(os.path.getctime(root)).isoformat()

            self.store_local_album_metadata(album_id, album_title, album_path, album_time)
            total_albums += 1
            current_album_files = 0

            # Process files in this directory
            for filename in media_files:
                filepath = os.path.join(root, filename)
                try:
                    metadata = get_file_metadata(filepath)
                    photo_id = os.path.relpath(filepath, self.local_photos_dir)

                    # Get image dimensions if possible
                    try:
                        width, height = get_image_dimensions(filepath)
                    except Exception as e:
                        logging.debug("Could not get dimensions for %s: %s", filepath, e)
                        width = height = None

                    photo_data = LocalPhotoData(
                        id=photo_id,
                        filename=filename,
                        normalized_filename=normalize_filename(filename),
                        path=filepath,
                        creation_time=metadata.creation_time,
                        width=width,
                        height=height,
                        mime_type=metadata.mime_type,
                    )
                    self.store_local_photo_metadata(photo_data)
                    self.store_local_album_photo_relation(album_id, photo_id)
                    total_files += 1
                    current_album_files += 1
                    print(
                        f"\rProcessed Total: {total_files:5d} files across {total_albums:5d} albums with {current_album_files:5d} files in {album_title:50s} ",
                        end="",
                        flush=True,
                    )
                except Exception as e:
                    logging.debug("Skipping file %s: %s", filepath, e)

        print()  # New line after progress
        logging.info(
            "Processed %d files across %d albums.",
            total_files,
            total_albums,
        )


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Google Photos Organizer")

    # Global arguments
    parser.add_argument("--local-photos-dir", type=str, help="Local photos directory")
    parser.add_argument("--dry-run", action="store_true", help="Run without making changes")

    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Commands", required=True)

    # Scan Google Photos command
    scan_google_parser = subparsers.add_parser("scan-google", help="Scan Google Photos")
    scan_google_parser.add_argument(
        "--max-photos", type=int, help="Maximum number of photos to scan"
    )

    # Scan local directory command
    scan_local_parser = subparsers.add_parser("scan-local", help="Scan local directory")
    scan_local_parser.add_argument(
        "--local-photos-dir",
        type=str,
        help="Local photos directory (overrides global setting)",
    )

    # Search command
    search_parser = subparsers.add_parser("search", help="Search photos")
    search_parser.add_argument("pattern", type=str, help="Filename pattern to search for")

    # Match command
    match_parser = subparsers.add_parser(
        "match", help="Find matching photos between local and Google Photos"
    )
    match_parser.add_argument(
        "--album-filter", help="Filter by album title (supports glob patterns)", default=None
    )

    # All command
    subparsers.add_parser("all", help="Run all commands")

    return parser.parse_args()


def main() -> None:
    """Main entry point for the Google Photos Organizer CLI."""
    args = parse_arguments()
    organizer = GooglePhotosOrganizer(
        local_photos_dir=args.local_photos_dir,
        dry_run=args.dry_run,
    )

    if args.command == "scan-google":
        organizer.authenticate()
        organizer.store_photos_and_albums(max_photos=args.max_photos)

    elif args.command == "scan-local":
        if not args.local_photos_dir:
            print("Please specify --local-photos-dir")
            return
        organizer.scan_local_directory()

    elif args.command == "search":
        organizer.search_files(args.pattern)

    elif args.command == "match":
        organizer.print_matching_photos(args.album_filter)

    elif args.command == "all":
        if not args.local_photos_dir:
            print("Please specify --local-photos-dir for full scan")
            return
        organizer.authenticate()
        organizer.store_photos_and_albums(max_photos=args.max_photos)
        organizer.scan_local_directory()
    else:
        parser = argparse.ArgumentParser(description="Google Photos Organizer")
        parser.print_help()


if __name__ == "__main__":
    main()
