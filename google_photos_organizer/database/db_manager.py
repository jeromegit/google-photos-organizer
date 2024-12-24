"""Database operations for Google Photos Organizer."""

import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any


class DatabaseError(Exception):
    """Database error exception."""


class DatabaseManager:
    """Manages database operations for Google Photos Organizer."""

    def __init__(self, db_path: str = 'photos.db', dry_run: bool = False):
        """Initialize database manager.

        Args:
            db_path: Path to SQLite database file
            dry_run: If True, show SQL operations without executing them
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.dry_run = dry_run

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
                display_sql = sql.replace('?', '%s')
                # Format parameters for display
                formatted_params = tuple(repr(p) if isinstance(p, str) else str(p) for p in params)
                print(f"Would execute: {display_sql % formatted_params}")
            else:
                print(f"Would execute: {sql}")
            return

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

    def init_database(self) -> None:
        """Initialize database tables for Google Photos data."""
        if not self.conn or not self.cursor:
            self.connect()

        try:
            # Create albums table
            self._execute('''
                CREATE TABLE IF NOT EXISTS albums (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    creation_time TEXT,
                    media_item_count INTEGER
                )
            ''')

            # Create photos table
            self._execute('''
                CREATE TABLE IF NOT EXISTS photos (
                    id TEXT PRIMARY KEY,
                    filename TEXT,
                    normalized_filename TEXT,
                    mime_type TEXT,
                    creation_time TEXT,
                    width INTEGER,
                    height INTEGER,
                    product_url TEXT
                )
            ''')

            # Create album_photos table for many-to-many relationship
            self._execute('''
                CREATE TABLE IF NOT EXISTS album_photos (
                    album_id TEXT,
                    photo_id TEXT,
                    FOREIGN KEY (album_id) REFERENCES albums (id),
                    FOREIGN KEY (photo_id) REFERENCES photos (id),
                    PRIMARY KEY (album_id, photo_id)
                )
            ''')

            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to initialize database: {e}") from e

    def init_local_tables(self) -> None:
        """Initialize database tables for local files."""
        if not self.conn or not self.cursor:
            self.connect()

        try:
            # Create local_albums table
            self._execute('''
                CREATE TABLE IF NOT EXISTS local_albums (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    full_path TEXT,
                    creation_time TEXT,
                    media_item_count INTEGER
                )
            ''')

            # Create local_photos table
            self._execute('''
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

            # Create local_album_photos table
            self._execute('''
                CREATE TABLE IF NOT EXISTS local_album_photos (
                    album_id TEXT,
                    photo_id TEXT,
                    FOREIGN KEY (album_id) REFERENCES local_albums (id),
                    FOREIGN KEY (photo_id) REFERENCES local_photos (id),
                    PRIMARY KEY (album_id, photo_id)
                )
            ''')

            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to initialize local tables: {e}") from e

    @dataclass
    class AlbumData:
        """Data class for album information."""
        id: str
        title: str
        creation_time: str

    def store_album(self, album_data: AlbumData) -> None:
        """Store album data in database.

        Args:
            album_data: Album data from Google Photos API
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                INSERT OR REPLACE INTO albums (id, title, creation_time)
                VALUES (?, ?, ?)
            ''', (album_data.id, album_data.title, album_data.creation_time))
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store album: {e}") from e

    @dataclass
    class PhotoData:
        """Data class for photo information."""
        id: str
        filename: str
        normalized_filename: str
        mime_type: str
        creation_time: str
        width: int
        height: int
        product_url: str

    def store_photo(self, photo_data: PhotoData) -> None:
        """Store photo data in database.

        Args:
            photo_data: Photo data from Google Photos API
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                INSERT OR REPLACE INTO photos (
                    id, filename, normalized_filename, mime_type,
                    creation_time, width, height, product_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                photo_data.id,
                photo_data.filename,
                photo_data.normalized_filename,
                photo_data.mime_type,
                photo_data.creation_time,
                photo_data.width,
                photo_data.height,
                photo_data.product_url
            ))
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store photo: {e}") from e

    def store_album_photo(self, album_id: str, photo_id: str) -> None:
        """Store album-photo relationship in database.

        Args:
            album_id: Album ID
            photo_id: Photo ID
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                INSERT OR REPLACE INTO album_photos (album_id, photo_id)
                VALUES (?, ?)
            ''', (album_id, photo_id))
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store album-photo relationship: {e}") from e

    @dataclass
    class LocalAlbumData:
        """Data class for local album information."""
        id: str
        title: str
        full_path: str
        creation_time: str

    def store_local_album(self, album_data: LocalAlbumData) -> None:
        """Store local album data in database.

        Args:
            album_data: Local album data
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                INSERT OR REPLACE INTO local_albums (
                    id, title, full_path, creation_time
                )
                VALUES (?, ?, ?, ?)
            ''', (
                album_data.id,
                album_data.title,
                album_data.full_path,
                album_data.creation_time
            ))
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store local album: {e}") from e

    @dataclass
    class LocalPhotoData:
        """Data class for local photo information."""
        id: str
        filename: str
        normalized_filename: str
        full_path: str
        creation_time: str
        mime_type: str
        size: int
        width: int
        height: int

    def store_local_photo(self, photo_data: LocalPhotoData) -> None:
        """Store local photo data in database.

        Args:
            photo_data: Local photo data
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                INSERT OR REPLACE INTO local_photos (
                    id, filename, normalized_filename, full_path,
                    creation_time, mime_type, size, width, height
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                photo_data.id,
                photo_data.filename,
                photo_data.normalized_filename,
                photo_data.full_path,
                photo_data.creation_time,
                photo_data.mime_type,
                photo_data.size,
                photo_data.width,
                photo_data.height
            ))
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to store local photo: {e}") from e

    def store_local_album_photo(self, album_id: str, photo_id: str) -> None:
        """Store local album-photo relationship in database.

        Args:
            album_id: Album ID
            photo_id: Photo ID
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                INSERT OR REPLACE INTO local_album_photos (album_id, photo_id)
                VALUES (?, ?)
            ''', (album_id, photo_id))
            self._commit()
        except sqlite3.Error as e:
            raise DatabaseError(
                f"Failed to store local album-photo relationship: {e}"
            ) from e

    def get_google_album(self, title: str) -> Optional[Dict[str, Any]]:
        """Get Google Photos album by title.

        Args:
            title: Album title

        Returns:
            Album data if found, None otherwise
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                SELECT id, title, creation_time
                FROM albums
                WHERE title = ?
            ''', (title,))
            row = self.cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'title': row[1],
                    'creation_time': row[2]
                }
            return None
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get Google album: {e}") from e

    def get_missing_files(
        self,
        local_album_id: str,
        google_album_id: str
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
            self._execute('''
                SELECT lp.filename, lp.width, lp.height
                FROM local_photos lp
                JOIN local_album_photos lap ON lp.id = lap.photo_id
                WHERE lap.album_id = ?
                AND NOT EXISTS (
                    SELECT 1
                    FROM photos p
                    JOIN album_photos ap ON p.id = ap.photo_id
                    WHERE ap.album_id = ?
                    AND p.normalized_filename = lp.normalized_filename
                )
            ''', (local_album_id, google_album_id))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to get missing files: {e}") from e

    def search_local_photos(
        self,
        query: str,
        normalized_query: str
    ) -> List[Tuple]:
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
            self._execute('''
                SELECT DISTINCT
                    'local' as source,
                    lp.filename,
                    lp.normalized_filename,
                    lp.width,
                    lp.height,
                    lp.creation_time,
                    GROUP_CONCAT(la.title, ' | ') as albums
                FROM local_photos lp
                LEFT JOIN local_album_photos lap ON lp.id = lap.photo_id
                LEFT JOIN local_albums la ON lap.album_id = la.id
                WHERE lp.filename LIKE ? OR lp.normalized_filename LIKE ?
                GROUP BY lp.id
            ''', (f'%{query}%', f'%{normalized_query}%'))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to search local photos: {e}") from e

    def search_google_photos(
        self,
        query: str,
        normalized_query: str
    ) -> List[Tuple]:
        """Search for photos in Google Photos database.

        Args:
            query: Search query
            normalized_query: Normalized search query

        Returns:
            List of matching photos with their metadata
        """
        if not self.conn or not self.cursor:
            self.connect()

        try:
            self._execute('''
                SELECT DISTINCT
                    'google' as source,
                    p.filename,
                    p.normalized_filename,
                    p.width,
                    p.height,
                    p.creation_time,
                    GROUP_CONCAT(a.title, ' | ') as albums
                FROM photos p
                LEFT JOIN album_photos ap ON p.id = ap.photo_id
                LEFT JOIN albums a ON ap.album_id = a.id
                WHERE p.filename LIKE ? OR p.normalized_filename LIKE ?
                GROUP BY p.id
            ''', (f'%{query}%', f'%{normalized_query}%'))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to search Google photos: {e}") from e
