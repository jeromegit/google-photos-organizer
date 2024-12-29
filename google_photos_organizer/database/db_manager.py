"""Database operations for Google Photos Organizer."""

import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union

from google_photos_organizer.database.models import (
    GoogleAlbumData,
    GooglePhotoData,
    LocalAlbumData,
    LocalPhotoData,
    PhotoSource,
)


class DatabaseError(Exception):
    """Database error exception."""


class DatabaseManager:
    """Manages database operations for Google Photos Organizer."""

    def __init__(self, db_path: str = "photos.db", dry_run: bool = False):
        """Initialize database manager.

        Args:
            db_path: Path to SQLite database file
            dry_run: If True, show SQL operations without executing them
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.dry_run = dry_run

    def _get_table_prefix(self, source: PhotoSource) -> str:
        """Get table prefix based on source.

        Args:
            source: Photo source (local or google)

        Returns:
            Table prefix to use
        """
        return f"{source.value}_"

    def _execute(self, sql: str, params: Tuple[Any, ...] = None) -> None:
        """Execute SQL with optional dry run mode.

        Args:
            sql: SQL query to execute
            params: Query parameters
        """
        if self.dry_run:
            # Format the SQL with parameters for display
            if params:
                # Replace ? with %s for string formatting
                sql_formatted = sql.replace("?", "%s")
                print(f"[DRY RUN] Would execute: {sql_formatted % params}")
            else:
                print(f"[DRY RUN] Would execute: {sql}")
            return

        if not self.conn or not self.cursor:
            self.connect()

        if params:
            self.cursor.execute(sql, params)
        else:
            self.cursor.execute(sql)

    def _commit(self) -> None:
        """Commit transaction with dry run support."""
        if self.dry_run:
            print("Would commit transaction")
            return
        self.conn.commit()

    def connect(self) -> None:
        """Connect to the database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to connect to database: {e}") from e

    def init_database(self, source: Optional[PhotoSource] = None) -> None:
        """Initialize the database tables.

        Args:
            source: If provided, only drop and recreate tables for this source
        """
        try:
            sources = [source] if source else list(PhotoSource)
            for src in sources:
                prefix = self._get_table_prefix(src)

                self._execute(f"DROP TABLE IF EXISTS {prefix}album_photos")
                self._execute(f"DROP TABLE IF EXISTS {prefix}photos")
                self._execute(f"DROP TABLE IF EXISTS {prefix}albums")

                # Create tables only if they don't exist
                self._execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {prefix}photos (
                        id TEXT PRIMARY KEY,
                        filename TEXT NOT NULL,
                        normalized_filename TEXT NOT NULL,
                        creation_time TEXT NOT NULL,
                        mime_type TEXT NOT NULL,
                        width INTEGER,
                        height INTEGER,
                        path TEXT NOT NULL
                    )
                """
                )

                self._execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {prefix}albums (
                        id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        creation_time TEXT NOT NULL,
                        path TEXT NOT NULL
                    )
                """
                )

                self._execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {prefix}album_photos (
                        album_id TEXT NOT NULL,
                        photo_id TEXT NOT NULL,
                        FOREIGN KEY (album_id) REFERENCES {prefix}albums (id),
                        FOREIGN KEY (photo_id) REFERENCES {prefix}photos (id),
                        PRIMARY KEY (album_id, photo_id)
                    )
                """
                )

                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{prefix}filename
                    ON {prefix}photos(filename)
                """
                )

                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{prefix}normalized_filename
                    ON {prefix}photos(normalized_filename)
                """
                )

            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to initialize database: {e}") from e

    def store_photo(
        self, photo_data: Union[GooglePhotoData, LocalPhotoData], source: PhotoSource
    ) -> None:
        """Store photo metadata in the database.

        Args:
            photo_data: Photo metadata to store
            source: Source of the photo (local or google)
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            prefix = self._get_table_prefix(source)
            self._execute(
                f"""
                INSERT OR REPLACE INTO {prefix}photos (
                    id, filename, normalized_filename, mime_type,
                    creation_time, width, height, path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    photo_data.id,
                    photo_data.filename,
                    photo_data.normalized_filename,
                    photo_data.mime_type,
                    photo_data.creation_time,
                    photo_data.width,
                    photo_data.height,
                    photo_data.path,
                ),
            )
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store photo: {e}") from e

    def store_album(
        self, album_data: Union[GoogleAlbumData, LocalAlbumData], source: PhotoSource
    ) -> None:
        """Store album metadata in the database."""
        if not self.conn or not self.cursor:
            self.connect()

        try:
            prefix = self._get_table_prefix(source)
            sql = f"""
                INSERT OR REPLACE INTO {prefix}albums (id, title, creation_time, path)
                VALUES (?, ?, ?, ?)
            """
            params = [
                album_data.id,
                album_data.title,
                album_data.creation_time,
                (
                    album_data.path if hasattr(album_data, "path") else ""
                ),  # Use empty string if path not present
            ]
            self._execute(sql, params)
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store album: {e}") from e

    def store_album_photo(self, album_id: str, photo_id: str, source: PhotoSource) -> None:
        """Store album-photo relationship in database.

        Args:
            album_id: Album ID
            photo_id: Photo ID
            source: Source of the album/photo (local or google)
        """
        try:
            prefix = self._get_table_prefix(source)
            self._execute(
                f"""
                INSERT OR REPLACE INTO {prefix}album_photos (
                    album_id, photo_id
                ) VALUES (?, ?)
                """,
                (album_id, photo_id),
            )
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store album-photo relationship: {e}") from e

    def get_album(self, title: str, source: PhotoSource) -> Optional[Dict]:
        """Get album by title.

        Args:
            title: Album title
            source: Source of the album (local or google)

        Returns:
            Album data or None if not found
        """
        try:
            prefix = self._get_table_prefix(source)
            if source == PhotoSource.LOCAL:
                self._execute(
                    f"""
                    SELECT id, title, creation_time, path
                    FROM {prefix}albums
                    WHERE title = ?
                    """,
                    (title,),
                )
            else:
                self._execute(
                    f"""
                    SELECT id, title, creation_time
                    FROM {prefix}albums
                    WHERE title = ?
                    """,
                    (title,),
                )
            row = self.cursor.fetchone()
            if row:
                if source == PhotoSource.LOCAL:
                    return {"id": row[0], "title": row[1], "creation_time": row[2], "path": row[3]}
                else:
                    return {"id": row[0], "title": row[1], "creation_time": row[2]}
            return None
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get album: {e}") from e

    def _has_column(self, table: str, column: str) -> bool:
        """Check if a table has a specific column.

        Args:
            table: Table name
            column: Column name

        Returns:
            True if column exists, False otherwise
        """
        try:
            self._execute(f"SELECT {column} FROM {table} LIMIT 0")
            return True
        except sqlite3.OperationalError:
            return False

    def search_photos(self, filename_pattern: str, normalized_pattern: str) -> List[Tuple]:
        """Search for photos in the database."""
        try:
            # Build union query for all sources
            queries = []
            params = []

            for source in PhotoSource:
                prefix = self._get_table_prefix(source)
                queries.append(
                    f"""
                    SELECT
                        '{source.value}' as source,
                        p.filename,
                        p.normalized_filename,
                        p.creation_time,
                        p.mime_type,
                        p.width,
                        p.height,
                        GROUP_CONCAT(DISTINCT a.title) as albums
                    FROM {prefix}photos p
                    LEFT JOIN {prefix}album_photos ap ON p.id = ap.photo_id
                    LEFT JOIN {prefix}albums a ON ap.album_id = a.id
                    WHERE p.filename LIKE ? OR p.normalized_filename LIKE ?
                    GROUP BY p.id
                """
                )
                params.extend([f"%{filename_pattern}%", f"%{normalized_pattern}%"])

            # Combine all queries with UNION ALL
            sql = " UNION ALL ".join(queries)
            self._execute(f"SELECT * FROM ({sql}) ORDER BY normalized_filename", tuple(params))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to search photos: {e}") from e

    def get_missing_files(
        self, local_album_id: str, google_album_id: str
    ) -> List[Tuple[str, int, int]]:
        """Get files that exist locally but not in Google Photos.

        Args:
            local_album_id: Local album ID
            google_album_id: Google Photos album ID

        Returns:
            List of tuples containing filename, width, and height
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute(
                """
                SELECT lp.filename, lp.width, lp.height
                FROM local_photos lp
                JOIN local_album_photos lap ON lp.id = lap.photo_id
                WHERE lap.album_id = ?
                AND NOT EXISTS (
                    SELECT 1
                    FROM google_photos p
                    JOIN google_album_photos ap ON p.id = ap.photo_id
                    WHERE ap.album_id = ?
                    AND p.normalized_filename = lp.normalized_filename
                )
            """,
                (local_album_id, google_album_id),
            )
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get missing files: {e}") from e

    def search_local_photos(self, query: str, normalized_query: str) -> List[Tuple]:
        """Search for photos in local database.

        Args:
            query: Search query
            normalized_query: Normalized search query

        Returns:
            List of matching photos with their metadata
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute(
                """
                SELECT DISTINCT
                    'local' as source,
                    lp.filename,
                    lp.normalized_filename,
                    lp.width,
                    lp.height,
                    lp.creation_time,
                    lp.full_path,
                    lp.size,
                    GROUP_CONCAT(la.title, ' | ') as albums
                FROM local_photos lp
                LEFT JOIN local_album_photos lap ON lp.id = lap.photo_id
                LEFT JOIN local_albums la ON lap.album_id = la.id
                WHERE lp.filename LIKE ? OR lp.normalized_filename LIKE ?
                GROUP BY lp.id
            """,
                (f"%{query}%", f"%{normalized_query}%"),
            )
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to search local photos: {e}") from e

    def create_indices(self, source: Optional[PhotoSource] = None) -> None:
        """Create indices for better query performance.

        Args:
            source: Optional source to create indices for. If None, creates indices for all sources.
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            sources = [source] if source else [PhotoSource.LOCAL, PhotoSource.GOOGLE]

            for src in sources:
                prefix = self._get_table_prefix(src)

                # Create indices on photos table
                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {prefix}photos_filename_idx
                    ON {prefix}photos(filename)
                """
                )
                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {prefix}photos_normalized_filename_idx
                    ON {prefix}photos(normalized_filename)
                """
                )
                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {prefix}photos_creation_time_idx
                    ON {prefix}photos(creation_time)
                """
                )

                # Create indices on albums table
                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {prefix}albums_title_idx
                    ON {prefix}albums(title)
                """
                )
                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {prefix}albums_creation_time_idx
                    ON {prefix}albums(creation_time)
                """
                )

                # Create indices on album_photos table
                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {prefix}album_photos_photo_id_idx
                    ON {prefix}album_photos(photo_id)
                """
                )
                self._execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {prefix}album_photos_album_id_idx
                    ON {prefix}album_photos(album_id)
                """
                )

            self._commit()
            print(f"Created indices for {', '.join(src.value for src in sources)} photos")

        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to create indices: {e}") from e

    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        try:
            self._execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to list tables: {e}") from e

    def count_photos(self, source: PhotoSource) -> int:
        """Count photos in a specific source."""
        try:
            prefix = self._get_table_prefix(source)
            self._execute(f"SELECT COUNT(*) FROM {prefix}photos")
            return self.cursor.fetchone()[0]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to count photos: {e}") from e

    def clear_data(self, source: PhotoSource) -> None:
        """Clear all data for a given source.

        Args:
            source: Source to clear data for
        """
        try:
            if source == PhotoSource.GOOGLE:
                self._execute("DELETE FROM google_photos")
                self._execute("DELETE FROM albums")
                self._execute("DELETE FROM album_photos")
            else:
                self._execute("DELETE FROM local_photos")
                self._execute("DELETE FROM local_albums")
                self._execute("DELETE FROM local_album_photos")
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to clear {source.name} data: {e}") from e

    def get_local_albums(self) -> List[Dict[str, Any]]:
        """Get all local albums."""
        try:
            query = "SELECT * FROM local_albums"
            self._execute(query)
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get local albums: {e}") from e

    def get_photos_in_local_album(self, album_id: str) -> List[Dict[str, Any]]:
        """Get all photos in a local album."""
        try:
            query = """
                SELECT p.* FROM local_photos p
                JOIN local_album_photos ap ON p.id = ap.photo_id
                WHERE ap.album_id = ?
            """
            self._execute(query, (album_id,))
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get photos in local album: {e}") from e

    def get_photo_count_in_local_album(self, album_id: str) -> int:
        """Get number of photos in a local album."""
        try:
            query = """
                SELECT COUNT(*) FROM local_album_photos
                WHERE album_id = ?
            """
            self._execute(query, (album_id,))
            return self.cursor.fetchone()[0]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get photo count in local album: {e}") from e

    def get_photo_count_in_album(self, album_id: str) -> int:
        """Get number of photos in a Google album."""
        try:
            query = """
                SELECT COUNT(*) FROM album_photos
                WHERE album_id = ?
            """
            self._execute(query, (album_id,))
            return self.cursor.fetchone()[0]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get photo count in album: {e}") from e

    def get_album_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Get a Google album by its title."""
        try:
            query = "SELECT * FROM albums WHERE title = ?"
            self._execute(query, (title,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get album by title: {e}") from e

    def get_missing_files_in_album(self, local_album_id: str, google_album_id: str) -> List[Dict[str, Any]]:
        """Get files that are in the local album but not in the Google album."""
        try:
            query = """
                SELECT p.* FROM local_photos p
                JOIN local_album_photos lap ON p.id = lap.photo_id
                LEFT JOIN album_photos ap ON p.id = ap.photo_id
                WHERE lap.album_id = ? AND ap.album_id != ?
            """
            self._execute(query, (local_album_id, google_album_id))
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get missing files in album: {e}") from e

    def get_photos_in_album(self, album_id: str) -> List[Dict[str, Any]]:
        """Get all photos in a Google album."""
        try:
            query = """
                SELECT p.* FROM google_photos p
                JOIN album_photos ap ON p.id = ap.photo_id
                WHERE ap.album_id = ?
            """
            self._execute(query, (album_id,))
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get photos in album: {e}") from e

    def get_google_albums(self) -> List[Dict[str, Any]]:
        """Get all Google albums."""
        try:
            query = "SELECT * FROM albums"
            self._execute(query)
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get Google albums: {e}") from e

    def get_local_photos(self, album_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get local photos, optionally filtered by album title.

        Args:
            album_filter: Optional album title to filter by

        Returns:
            List of dictionaries containing local photo data
        """
        try:
            query = """
                SELECT DISTINCT
                    lp.id,
                    lp.filename,
                    lp.normalized_filename,
                    lp.width,
                    lp.height,
                    la.title as album_title
                FROM local_photos lp
                JOIN local_album_photos lap ON lp.id = lap.photo_id
                JOIN local_albums la ON lap.album_id = la.id
            """
            params = []
            if album_filter:
                query += " WHERE la.title LIKE ?"
                params.append(f"%{album_filter}%")

            query += " ORDER BY la.title, lp.normalized_filename"
            
            self._execute(query, tuple(params) if params else None)
            return [
                {
                    'id': row[0],
                    'filename': row[1],
                    'normalized_filename': row[2],
                    'width': row[3],
                    'height': row[4],
                    'album_title': row[5]
                }
                for row in self.cursor.fetchall()
            ]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get local photos: {e}") from e

    def find_google_photos_by_filename(self, normalized_filename: str) -> List[Dict[str, Any]]:
        """Find Google photos matching a normalized filename.

        Args:
            normalized_filename: Normalized filename to match

        Returns:
            List of matching Google photo data
        """
        try:
            self._execute(
                """
                SELECT
                    id,
                    filename,
                    width,
                    height
                FROM google_photos
                WHERE normalized_filename = ?
                """,
                (normalized_filename,)
            )
            return [
                {
                    'id': row[0],
                    'filename': row[1],
                    'width': row[2],
                    'height': row[3]
                }
                for row in self.cursor.fetchall()
            ]
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to find Google photos: {e}") from e

    def get_photo_by_filename_and_dimensions(
        self,
        normalized_filename: str,
        width: int,
        height: int
    ) -> Optional[Dict[str, Any]]:
        """Get a photo by its normalized filename and dimensions."""
        try:
            query = """
                SELECT * FROM google_photos
                WHERE normalized_filename = ?
                AND width = ? AND height = ?
            """
            self._execute(query, (normalized_filename, width, height))
            row = self.cursor.fetchone()
            if row:
                return dict(row)
            return None
        except sqlite3.Error as e:
            raise DatabaseError(
                f"Failed to get photo by filename and dimensions: {e}"
            ) from e
