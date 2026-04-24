"""
Configuration loader. Reads from .env at project root.

Import pattern:
    from src.utils.config import settings
    settings.SNOWFLAKE_USER
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root regardless of where Python is invoked.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    # Snowflake
    SNOWFLAKE_ACCOUNT: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    SNOWFLAKE_USER: str = os.getenv("SNOWFLAKE_USER", "")
    SNOWFLAKE_PASSWORD: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    SNOWFLAKE_ROLE: str = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    SNOWFLAKE_WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    SNOWFLAKE_DATABASE: str = os.getenv("SNOWFLAKE_DATABASE", "JOBSEEKERS")
    SNOWFLAKE_SCHEMA_BRONZE: str = os.getenv("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
    SNOWFLAKE_SCHEMA_SILVER: str = os.getenv("SNOWFLAKE_SCHEMA_SILVER", "SILVER")
    SNOWFLAKE_SCHEMA_GOLD: str = os.getenv("SNOWFLAKE_SCHEMA_GOLD", "GOLD")

    # Adzuna
    ADZUNA_APP_ID: str = os.getenv("ADZUNA_APP_ID", "")
    ADZUNA_APP_KEY: str = os.getenv("ADZUNA_APP_KEY", "")
    ADZUNA_COUNTRY: str = os.getenv("ADZUNA_COUNTRY", "us")

    # USAJobs
    USAJOBS_API_KEY: str = os.getenv("USAJOBS_API_KEY", "")
    USAJOBS_USER_AGENT: str = os.getenv("USAJOBS_USER_AGENT", "")


settings = Settings()


def require(*names: str) -> None:
    """Raise if any of the given settings are empty. Call at the top of a script
    that needs specific credentials.
    """
    missing = [n for n in names if not getattr(settings, n, "")]
    if missing:
        raise RuntimeError(
            f"Missing required settings in .env: {', '.join(missing)}"
        )
