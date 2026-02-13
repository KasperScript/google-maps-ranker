"""SQLite cache for Places and Routes API responses."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_request_cache_key(url: str, field_mask: str, body: Dict[str, Any]) -> str:
    payload = json.dumps(body, sort_keys=True, separators=(",", ":"))
    raw = f"{url}|{field_mask}|{payload}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class Cache:
    def __init__(self, db_path: str, commit_every: int = 50) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._pending_writes = 0
        self._commit_every = max(1, int(commit_every))
        self._configure_conn()
        self._init_db()

    def _configure_conn(self) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL")
            cur.fetchone()
        except sqlite3.DatabaseError:
            pass
        try:
            cur.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.DatabaseError:
            pass

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS places_search_cache (
                key TEXT PRIMARY KEY,
                response_json TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS places_canonical (
                place_id TEXT PRIMARY KEY,
                name TEXT,
                rating REAL,
                user_rating_count INTEGER,
                lat REAL,
                lon REAL,
                business_status TEXT,
                types_json TEXT,
                found_by_json TEXT,
                last_seen_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS routes_cache (
                key TEXT PRIMARY KEY,
                origin_id TEXT,
                place_id TEXT,
                mode TEXT,
                duration_seconds INTEGER,
                created_at TEXT
            )
            """
        )
        self.conn.commit()

    def _mark_dirty(self) -> None:
        self._pending_writes += 1
        if self._pending_writes >= self._commit_every:
            self.commit()

    def commit(self) -> None:
        if self._pending_writes:
            self.conn.commit()
            self._pending_writes = 0

    def close(self) -> None:
        self.commit()
        self.conn.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def get_search_cache(self, key: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute("SELECT response_json FROM places_search_cache WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row["response_json"])

    def set_search_cache(self, key: str, response: Dict[str, Any]) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO places_search_cache (key, response_json, created_at)
            VALUES (?, ?, ?)
            """,
            (key, json.dumps(response), utc_now_iso()),
        )
        self._mark_dirty()

    def upsert_place(self, place: Dict[str, Any]) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO places_canonical (
                place_id, name, rating, user_rating_count, lat, lon,
                business_status, types_json, found_by_json, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(place_id) DO UPDATE SET
                name = excluded.name,
                rating = excluded.rating,
                user_rating_count = excluded.user_rating_count,
                lat = excluded.lat,
                lon = excluded.lon,
                business_status = excluded.business_status,
                types_json = excluded.types_json,
                found_by_json = excluded.found_by_json,
                last_seen_at = excluded.last_seen_at
            """,
            (
                place["place_id"],
                place.get("name"),
                place.get("rating"),
                place.get("user_rating_count"),
                place.get("lat"),
                place.get("lon"),
                place.get("business_status"),
                json.dumps(place.get("types", [])),
                json.dumps(place.get("found_by", [])),
                utc_now_iso(),
            ),
        )
        self._mark_dirty()

    def get_all_places(self) -> Iterable[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM places_canonical")
        for row in cur.fetchall():
            yield {
                "place_id": row["place_id"],
                "name": row["name"],
                "rating": row["rating"],
                "user_rating_count": row["user_rating_count"],
                "lat": row["lat"],
                "lon": row["lon"],
                "business_status": row["business_status"],
                "types": json.loads(row["types_json"] or "[]"),
                "found_by": json.loads(row["found_by_json"] or "[]"),
                "last_seen_at": row["last_seen_at"],
            }

    def get_routes_cache(self, key: str) -> Optional[int]:
        cur = self.conn.cursor()
        cur.execute("SELECT duration_seconds FROM routes_cache WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return None
        return int(row["duration_seconds"]) if row["duration_seconds"] is not None else None

    def set_routes_cache(
        self, key: str, origin_id: str, place_id: str, mode: str, duration_seconds: Optional[int]
    ) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO routes_cache (key, origin_id, place_id, mode, duration_seconds, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (key, origin_id, place_id, mode, duration_seconds, utc_now_iso()),
        )
        self._mark_dirty()
