"""
Bronze → Silver transformer.

Why pure SQL (executed through the Snowflake connector) instead of PySpark
for this step?

    1. The data already lives in Snowflake; shipping it out to Spark and back
       just to run map/filter logic that Snowflake can do natively wastes time
       and money.
    2. For the scale of this project (a few million rows), Snowflake's
       VARIANT-path SQL is more than fast enough.
    3. We still use PySpark in the Silver → Gold step (star-schema building)
       where the distributed-transformation story is more genuine.

The helpers below run idempotent MERGE statements so we can re-run them safely.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.utils.config import settings, require

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL: Adzuna Bronze → Silver
# ---------------------------------------------------------------------------
# Key field mappings (Adzuna JSON → Silver columns):
#   id                       -> job_id         (prefixed with 'adzuna_')
#   title                    -> job_title
#   company.display_name     -> company_or_agency
#   description              -> job_description
#   location.area (array)    -> location_country / state / city
#   latitude / longitude     -> lat / lon
#   salary_min / salary_max  -> salary_min / salary_max
#   salary_is_predicted      -> salary_is_predicted
#   category.label           -> job_category
#   contract_time            -> employment_type
#   created                  -> posted_date
#
# Entry-level classification:
#   Uses simple regex on the title. USAJobs will use GS grades; here we
#   fall back to keyword heuristics that paper §4 discusses.

ADZUNA_MERGE_SQL = """
MERGE INTO SILVER.jobs_unified AS tgt
USING (
    SELECT
        'adzuna_' || job_id                              AS job_id,
        'adzuna'                                         AS source,
        'private'                                        AS sector,

        raw_data:title::STRING                           AS job_title,
        LOWER(REGEXP_REPLACE(raw_data:title::STRING, '[^a-zA-Z0-9 ]', ''))
                                                         AS job_title_normalized,
        raw_data:company.display_name::STRING            AS company_or_agency,
        raw_data:description::STRING                     AS job_description,

        -- location.area is an array ordered as [country, state, county/city, ...]
        -- For US jobs it's typically ['US', 'California', 'San Francisco'].
        raw_data:location.area[2]::STRING                AS location_city,
        raw_data:location.area[1]::STRING                AS location_state,
        raw_data:location.area[0]::STRING                AS location_country,
        raw_data:latitude::FLOAT                         AS latitude,
        raw_data:longitude::FLOAT                        AS longitude,

        raw_data:salary_min::FLOAT                       AS salary_min,
        raw_data:salary_max::FLOAT                       AS salary_max,
        COALESCE(raw_data:salary_is_predicted::STRING = '1', FALSE)
                                                         AS salary_is_predicted,

        raw_data:category.label::STRING                  AS job_category,

        -- Experience level heuristic
        CASE
            WHEN REGEXP_LIKE(LOWER(raw_data:title::STRING),
                             '.*(intern|internship|entry[- ]level|junior|graduate|new grad|associate).*')
                 THEN 'entry'
            WHEN REGEXP_LIKE(LOWER(raw_data:title::STRING),
                             '.*(senior|sr\\.|principal|staff|lead|director|head of|vp|chief).*')
                 THEN 'senior'
            ELSE 'mid'
        END                                              AS experience_level,

        REGEXP_LIKE(LOWER(raw_data:title::STRING),
                    '.*(intern|internship|entry[- ]level|junior|graduate|new grad).*')
                                                         AS is_entry_level,

        CASE
            WHEN raw_data:contract_time::STRING = 'full_time'  THEN 'full_time'
            WHEN raw_data:contract_time::STRING = 'part_time'  THEN 'part_time'
            WHEN REGEXP_LIKE(LOWER(raw_data:title::STRING), '.*intern.*') THEN 'intern'
            ELSE 'unknown'
        END                                              AS employment_type,

        -- Adzuna doesn't directly expose remote flag; infer from description.
        CASE
            WHEN REGEXP_LIKE(LOWER(raw_data:description::STRING),
                             '.*\\\\bremote\\\\b.*')        THEN 'remote'
            WHEN REGEXP_LIKE(LOWER(raw_data:description::STRING),
                             '.*\\\\bhybrid\\\\b.*')        THEN 'hybrid'
            ELSE 'unknown'
        END                                              AS remote_type,

        TO_DATE(raw_data:created::STRING)                AS posted_date,
        NULL                                             AS close_date,

        ingestion_timestamp                              AS ingestion_timestamp
    FROM BRONZE.raw_adzuna
) AS src
ON tgt.source = src.source AND tgt.job_id = src.job_id
WHEN MATCHED THEN UPDATE SET
    job_title            = src.job_title,
    job_title_normalized = src.job_title_normalized,
    company_or_agency    = src.company_or_agency,
    job_description      = src.job_description,
    location_city        = src.location_city,
    location_state       = src.location_state,
    location_country     = src.location_country,
    latitude             = src.latitude,
    longitude            = src.longitude,
    salary_min           = src.salary_min,
    salary_max           = src.salary_max,
    salary_is_predicted  = src.salary_is_predicted,
    job_category         = src.job_category,
    experience_level     = src.experience_level,
    is_entry_level       = src.is_entry_level,
    employment_type      = src.employment_type,
    remote_type          = src.remote_type,
    posted_date          = src.posted_date,
    close_date           = src.close_date,
    ingestion_timestamp  = src.ingestion_timestamp
WHEN NOT MATCHED THEN INSERT (
    job_id, source, sector,
    job_title, job_title_normalized, company_or_agency, job_description,
    location_city, location_state, location_country, latitude, longitude,
    salary_min, salary_max, salary_is_predicted,
    job_category, experience_level, is_entry_level,
    employment_type, remote_type,
    posted_date, close_date, ingestion_timestamp
) VALUES (
    src.job_id, src.source, src.sector,
    src.job_title, src.job_title_normalized, src.company_or_agency, src.job_description,
    src.location_city, src.location_state, src.location_country, src.latitude, src.longitude,
    src.salary_min, src.salary_max, src.salary_is_predicted,
    src.job_category, src.experience_level, src.is_entry_level,
    src.employment_type, src.remote_type,
    src.posted_date, src.close_date, src.ingestion_timestamp
)
"""


# ---------------------------------------------------------------------------
# SQL: USAJobs Current Bronze → Silver
# ---------------------------------------------------------------------------
# Key field mappings (USAJobs JSON → Silver columns):
#   MatchedObjectId                              -> job_id  (prefixed 'usajobs_')
#   MatchedObjectDescriptor.PositionTitle        -> job_title
#   MatchedObjectDescriptor.OrganizationName     -> company_or_agency
#   MatchedObjectDescriptor.PositionFormattedDescription[0].Content
#     (falls back to QualificationSummary)       -> job_description
#   MatchedObjectDescriptor.PositionLocation[0]  -> location_city/state/country/lat/lon
#   MatchedObjectDescriptor.PositionRemuneration[0].MinimumRange/MaximumRange
#     (annualized: PA=as-is, PH=×2080, PB=×26)  -> salary_min / salary_max
#   MatchedObjectDescriptor.JobCategory[0].Name  -> job_category
#   MatchedObjectDescriptor.JobGrade[0].Code      -> used for SES→'senior' shortcut
#   MatchedObjectDescriptor.PositionSchedule[0].Code
#     ('1'=full_time, '2'=part_time)              -> employment_type
#   MatchedObjectDescriptor.RemoteIndicator       -> remote_type
#   MatchedObjectDescriptor.PublicationStartDate  -> posted_date
#   MatchedObjectDescriptor.ApplicationCloseDate  -> close_date
#
# Experience level: SES grade → senior; title keywords → entry/senior;
#   annualized salary_min (<55k → entry, ≥100k → senior); default → mid.
# Remote type: dedicated RemoteIndicator boolean beats keyword inference.

USAJOBS_MERGE_SQL = """
MERGE INTO SILVER.jobs_unified AS tgt
USING (
    SELECT
        'usajobs_' || control_number                     AS job_id,
        'usajobs_current'                                AS source,
        'federal'                                        AS sector,

        raw_data:MatchedObjectDescriptor.PositionTitle::STRING
                                                         AS job_title,
        LOWER(REGEXP_REPLACE(
            raw_data:MatchedObjectDescriptor.PositionTitle::STRING,
            '[^a-zA-Z0-9 ]', ''))                        AS job_title_normalized,
        raw_data:MatchedObjectDescriptor.OrganizationName::STRING
                                                         AS company_or_agency,
        COALESCE(
            raw_data:MatchedObjectDescriptor.PositionFormattedDescription[0].Content::STRING,
            raw_data:MatchedObjectDescriptor.QualificationSummary::STRING
        )                                                AS job_description,

        -- PositionLocation is an array; take first entry for the primary location.
        -- CityName often looks like "Washington, District of Columbia" — split on comma.
        SPLIT_PART(
            raw_data:MatchedObjectDescriptor.PositionLocation[0].CityName::STRING,
            ',', 1)                                      AS location_city,
        raw_data:MatchedObjectDescriptor.PositionLocation[0].CountrySubDivisionCode::STRING
                                                         AS location_state,
        raw_data:MatchedObjectDescriptor.PositionLocation[0].CountryCode::STRING
                                                         AS location_country,
        raw_data:MatchedObjectDescriptor.PositionLocation[0].Latitude::FLOAT
                                                         AS latitude,
        raw_data:MatchedObjectDescriptor.PositionLocation[0].Longitude::FLOAT
                                                         AS longitude,

        salary_min_annual                                AS salary_min,
        salary_max_annual                                AS salary_max,
        FALSE                                            AS salary_is_predicted,

        raw_data:MatchedObjectDescriptor.JobCategory[0].Name::STRING
                                                         AS job_category,

        CASE
            WHEN grade_code = 'SES'
                 THEN 'senior'
            WHEN REGEXP_LIKE(title_lower,
                             '.*(intern|internship|entry[- ]level|junior|graduate|trainee).*')
                 THEN 'entry'
            WHEN REGEXP_LIKE(title_lower,
                             '.*(senior|sr\\.|principal|staff|lead|director|chief|executive).*')
                 THEN 'senior'
            WHEN salary_min_annual < 55000
                 THEN 'entry'
            WHEN salary_min_annual >= 100000
                 THEN 'senior'
            ELSE 'mid'
        END                                              AS experience_level,

        REGEXP_LIKE(title_lower,
                    '.*(intern|internship|entry[- ]level|junior|graduate|trainee).*')
                                                         AS is_entry_level,

        CASE
            WHEN raw_data:MatchedObjectDescriptor.PositionSchedule[0].Code::STRING = '1'
                 THEN 'full_time'
            WHEN raw_data:MatchedObjectDescriptor.PositionSchedule[0].Code::STRING = '2'
                 THEN 'part_time'
            WHEN REGEXP_LIKE(title_lower, '.*intern.*')
                 THEN 'intern'
            ELSE 'unknown'
        END                                              AS employment_type,

        -- USAJobs has a dedicated RemoteIndicator boolean, more reliable than
        -- keyword inference. Fall through to description scan for hybrid.
        CASE
            WHEN raw_data:MatchedObjectDescriptor.RemoteIndicator::BOOLEAN = TRUE
                 THEN 'remote'
            WHEN REGEXP_LIKE(
                    LOWER(COALESCE(
                        raw_data:MatchedObjectDescriptor.PositionFormattedDescription[0].Content::STRING,
                        raw_data:MatchedObjectDescriptor.QualificationSummary::STRING,
                        '')),
                    '.*\\\\bhybrid\\\\b.*')              THEN 'hybrid'
            ELSE 'onsite'
        END                                              AS remote_type,

        TO_DATE(raw_data:MatchedObjectDescriptor.PublicationStartDate::STRING)
                                                         AS posted_date,
        TO_DATE(raw_data:MatchedObjectDescriptor.ApplicationCloseDate::STRING)
                                                         AS close_date,

        ingestion_timestamp
    FROM (
        -- Inner query precomputes derived scalars so the outer SELECT can reference
        -- them in CASE expressions without repeating long VARIANT path expressions.
        SELECT
            control_number,
            raw_data,
            ingestion_timestamp,
            raw_data:MatchedObjectDescriptor.JobGrade[0].Code::STRING
                                                         AS grade_code,
            LOWER(raw_data:MatchedObjectDescriptor.PositionTitle::STRING)
                                                         AS title_lower,
            CASE raw_data:MatchedObjectDescriptor.PositionRemuneration[0].RateIntervalCode::STRING
                WHEN 'PA' THEN raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MinimumRange::FLOAT
                WHEN 'PH' THEN raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MinimumRange::FLOAT * 2080
                WHEN 'PB' THEN raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MinimumRange::FLOAT * 26
                ELSE           raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MinimumRange::FLOAT
            END                                          AS salary_min_annual,
            CASE raw_data:MatchedObjectDescriptor.PositionRemuneration[0].RateIntervalCode::STRING
                WHEN 'PA' THEN raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MaximumRange::FLOAT
                WHEN 'PH' THEN raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MaximumRange::FLOAT * 2080
                WHEN 'PB' THEN raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MaximumRange::FLOAT * 26
                ELSE           raw_data:MatchedObjectDescriptor.PositionRemuneration[0].MaximumRange::FLOAT
            END                                          AS salary_max_annual
        FROM BRONZE.raw_usajobs_current
    )
) AS src
ON tgt.source = src.source AND tgt.job_id = src.job_id
WHEN MATCHED THEN UPDATE SET
    job_title            = src.job_title,
    job_title_normalized = src.job_title_normalized,
    company_or_agency    = src.company_or_agency,
    job_description      = src.job_description,
    location_city        = src.location_city,
    location_state       = src.location_state,
    location_country     = src.location_country,
    latitude             = src.latitude,
    longitude            = src.longitude,
    salary_min           = src.salary_min,
    salary_max           = src.salary_max,
    salary_is_predicted  = src.salary_is_predicted,
    job_category         = src.job_category,
    experience_level     = src.experience_level,
    is_entry_level       = src.is_entry_level,
    employment_type      = src.employment_type,
    remote_type          = src.remote_type,
    posted_date          = src.posted_date,
    close_date           = src.close_date,
    ingestion_timestamp  = src.ingestion_timestamp
WHEN NOT MATCHED THEN INSERT (
    job_id, source, sector,
    job_title, job_title_normalized, company_or_agency, job_description,
    location_city, location_state, location_country, latitude, longitude,
    salary_min, salary_max, salary_is_predicted,
    job_category, experience_level, is_entry_level,
    employment_type, remote_type,
    posted_date, close_date, ingestion_timestamp
) VALUES (
    src.job_id, src.source, src.sector,
    src.job_title, src.job_title_normalized, src.company_or_agency, src.job_description,
    src.location_city, src.location_state, src.location_country, src.latitude, src.longitude,
    src.salary_min, src.salary_max, src.salary_is_predicted,
    src.job_category, src.experience_level, src.is_entry_level,
    src.employment_type, src.remote_type,
    src.posted_date, src.close_date, src.ingestion_timestamp
)
"""


class BronzeToSilverTransformer:
    """Runs idempotent MERGE statements that upsert Bronze rows into Silver."""

    def __init__(self) -> None:
        require(
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
        )

    @contextmanager
    def _connect(self) -> Iterator[SnowflakeConnection]:
        conn = snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            role=settings.SNOWFLAKE_ROLE,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
        )
        try:
            yield conn
        finally:
            conn.close()

    def transform_adzuna(self) -> int:
        """Upsert all current Bronze Adzuna rows into Silver.jobs_unified.

        Returns the number of rows affected by the MERGE.
        """
        logger.info("Running Adzuna Bronze→Silver MERGE")
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(ADZUNA_MERGE_SQL)
                # Snowflake's MERGE returns the number of rows inserted + updated
                # via result metadata.
                affected = cur.rowcount
                conn.commit()
            finally:
                cur.close()
        logger.info("Adzuna MERGE affected %s rows", affected)
        return affected or 0

    def transform_usajobs(self) -> int:
        """Upsert all current Bronze USAJobs rows into Silver.jobs_unified.

        Returns the number of rows affected by the MERGE.
        """
        logger.info("Running USAJobs Bronze→Silver MERGE")
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(USAJOBS_MERGE_SQL)
                affected = cur.rowcount
                conn.commit()
            finally:
                cur.close()
        logger.info("USAJobs MERGE affected %s rows", affected)
        return affected or 0
