"""
SQLite-backed user management.

Tables:
  users             — email (PK), name, hashed password
  user_preferences  — per-user filter preferences for the dashboard feed
  user_saved_jobs   — bookmarked jobs with title/company/apply_url cached

The database file is created automatically at analysis/users.db the first time
init_db() is called.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "users.db"


# ── connection ──────────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    """Create tables if they don't exist. Idempotent."""
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            email         TEXT PRIMARY KEY,
            name          TEXT NOT NULL,
            salt          TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS user_preferences (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email  TEXT    NOT NULL REFERENCES users(email) ON DELETE CASCADE,
            pref_type   TEXT    NOT NULL,
            pref_value  TEXT    NOT NULL,
            UNIQUE(user_email, pref_type, pref_value)
        );

        CREATE TABLE IF NOT EXISTS user_saved_jobs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email  TEXT    NOT NULL REFERENCES users(email) ON DELETE CASCADE,
            job_id      TEXT    NOT NULL,
            source      TEXT    NOT NULL,
            job_title   TEXT,
            company     TEXT,
            location    TEXT,
            apply_url   TEXT,
            saved_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_email, job_id, source)
        );
    """)
    conn.commit()
    conn.close()


# ── auth ─────────────────────────────────────────────────────────────────────

def _hash(password: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest()


def register_user(email: str, name: str, password: str) -> bool:
    """Return True on success, False if email already exists."""
    salt = os.urandom(16).hex()
    try:
        conn = _connect()
        conn.execute(
            "INSERT INTO users (email, name, salt, password_hash) VALUES (?,?,?,?)",
            (email.lower().strip(), name.strip(), salt, _hash(password, salt)),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def authenticate(email: str, password: str) -> dict | None:
    """Return user row dict on success, None on failure."""
    conn = _connect()
    row = conn.execute(
        "SELECT * FROM users WHERE email = ?", (email.lower().strip(),)
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return dict(row) if _hash(password, row["salt"]) == row["password_hash"] else None


# ── preferences ──────────────────────────────────────────────────────────────

def get_preferences(user_email: str) -> dict[str, list[str]]:
    """Return {pref_type: [values]} dict for the user."""
    conn = _connect()
    rows = conn.execute(
        "SELECT pref_type, pref_value FROM user_preferences WHERE user_email = ?",
        (user_email,),
    ).fetchall()
    conn.close()
    result: dict[str, list[str]] = {}
    for r in rows:
        result.setdefault(r["pref_type"], []).append(r["pref_value"])
    return result


def save_preferences(user_email: str, prefs: dict[str, list[str]]) -> None:
    """Overwrite all preferences for the user."""
    conn = _connect()
    conn.execute("DELETE FROM user_preferences WHERE user_email = ?", (user_email,))
    for pref_type, values in prefs.items():
        for val in values:
            conn.execute(
                "INSERT OR IGNORE INTO user_preferences (user_email, pref_type, pref_value)"
                " VALUES (?,?,?)",
                (user_email, pref_type, val),
            )
    conn.commit()
    conn.close()


# ── saved jobs ────────────────────────────────────────────────────────────────

def save_job(user_email: str, job: dict) -> bool:
    try:
        conn = _connect()
        conn.execute(
            "INSERT OR IGNORE INTO user_saved_jobs"
            " (user_email, job_id, source, job_title, company, location, apply_url)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                user_email,
                job.get("job_id", ""),
                job.get("source", ""),
                job.get("job_title"),
                job.get("company_or_agency"),
                f"{job.get('location_city','')}, {job.get('location_state','')}".strip(", "),
                job.get("apply_url"),
            ),
        )
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def get_saved_jobs(user_email: str) -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM user_saved_jobs WHERE user_email = ? ORDER BY saved_at DESC",
        (user_email,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
