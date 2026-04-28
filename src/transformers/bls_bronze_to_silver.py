"""
Bronze → Silver transformer for BLS datasets.

Mirrors the existing BronzeToSilverTransformer (for Adzuna/USAJobs) in style:
big SQL constants + thin Python wrapper that runs them as idempotent MERGEs.

Three transforms:

    transform_qcew()   BRONZE.raw_bls_qcew    -> SILVER.bls_qcew_quarterly
    transform_oews()   BRONZE.raw_bls_oews    -> SILVER.bls_oews_annual
    resolve_soc()      SILVER.jobs_unified    -> SILVER.dim_occupation_soc
                       (also writes back jobs_unified.soc_code for downstream
                       Gold builds)

The SOC resolver is the analytically-important one. It uses a small lookup
table that we seed with a curated subset of BLS's Direct Match Title File
(top 200 most-common job titles → SOC codes), plus a fuzzy fallback inside
Snowflake using EDITDISTANCE. That keeps the whole step inside the warehouse
— no Python-side rapidfuzz needed at runtime.
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
# QCEW: bronze -> silver
# ---------------------------------------------------------------------------
# Notes:
#   * area_type derived from agglvl_code (BLS aggregation level taxonomy):
#       10..14 = national, 50..58 = state, 70..78 = MSA, 30..39 = county, etc.
#   * own_code labels per BLS:
#       0=Total, 1=Federal, 2=State, 3=Local, 5=Private, 8=Total private+gov
#   * industry_level inferred from NAICS code length (special codes like
#     '10' or '1025' get level 0).
#   * GROUP BY collapses any duplicates from re-loaded bronze partitions;
#     MAX(ingestion_timestamp) wins.

QCEW_BRONZE_TO_SILVER_SQL = """
MERGE INTO SILVER.bls_qcew_quarterly AS tgt
USING (
    SELECT
        area_fips || '|' || industry_code || '|' || own_code
            || '|' || year || '|' || qtr                    AS qcew_key,
        area_fips,
        CASE
            WHEN agglvl_code BETWEEN 10 AND 19 THEN 'national'
            WHEN agglvl_code BETWEEN 50 AND 59 THEN 'state'
            WHEN agglvl_code BETWEEN 70 AND 79 THEN 'msa'
            WHEN agglvl_code BETWEEN 30 AND 39 THEN 'county'
            WHEN agglvl_code BETWEEN 90 AND 99 THEN 'csa'
            ELSE 'other'
        END                                                  AS area_type,
        CASE WHEN LENGTH(area_fips) >= 2 THEN LEFT(area_fips, 2) ELSE NULL END
                                                             AS state_fips,
        industry_code,
        CASE
            WHEN industry_code RLIKE '^[0-9]{2,6}$' THEN LENGTH(industry_code)
            ELSE 0
        END                                                  AS industry_level,
        own_code,
        CASE own_code
            WHEN 0 THEN 'Total'
            WHEN 1 THEN 'Federal'
            WHEN 2 THEN 'State'
            WHEN 3 THEN 'Local'
            WHEN 5 THEN 'Private'
            WHEN 8 THEN 'Total Private+Gov'
            ELSE 'Other'
        END                                                  AS own_label,
        year,
        qtr,
        MAX(qtrly_estabs)                                    AS qtrly_estabs,
        (COALESCE(MAX(month1_emplvl), 0)
            + COALESCE(MAX(month2_emplvl), 0)
            + COALESCE(MAX(month3_emplvl), 0)) / 3.0         AS avg_monthly_emplvl,
        MAX(total_qtrly_wages)                               AS total_qtrly_wages,
        MAX(avg_wkly_wage)                                   AS avg_wkly_wage,
        MAX(ingestion_timestamp)                             AS ingestion_timestamp
    FROM BRONZE.raw_bls_qcew
    -- Skip rows that BLS suppressed (no data behind the row).
    WHERE disclosure_code IS NULL OR disclosure_code = ''
    GROUP BY area_fips, industry_code, own_code, year, qtr, agglvl_code
) AS src
ON tgt.qcew_key = src.qcew_key
WHEN MATCHED AND src.ingestion_timestamp > tgt.ingestion_timestamp THEN UPDATE SET
    qtrly_estabs        = src.qtrly_estabs,
    avg_monthly_emplvl  = src.avg_monthly_emplvl,
    total_qtrly_wages   = src.total_qtrly_wages,
    avg_wkly_wage       = src.avg_wkly_wage,
    ingestion_timestamp = src.ingestion_timestamp
WHEN NOT MATCHED THEN INSERT (
    qcew_key, area_fips, area_type, state_fips,
    industry_code, industry_level, own_code, own_label,
    year, qtr,
    qtrly_estabs, avg_monthly_emplvl, total_qtrly_wages, avg_wkly_wage,
    ingestion_timestamp
) VALUES (
    src.qcew_key, src.area_fips, src.area_type, src.state_fips,
    src.industry_code, src.industry_level, src.own_code, src.own_label,
    src.year, src.qtr,
    src.qtrly_estabs, src.avg_monthly_emplvl, src.total_qtrly_wages, src.avg_wkly_wage,
    src.ingestion_timestamp
)
"""


# ---------------------------------------------------------------------------
# OEWS: bronze -> silver
# ---------------------------------------------------------------------------
# Bronze schema (after the 2026-04-28 fix): single VARIANT column `record`
# whose keys are the OEWS CSV header names (case-insensitive, populated via
# Snowflake's MATCH_BY_COLUMN_NAME). Suppression markers ('*', '**', '#')
# were already mapped to NULL by the file format's NULL_IF list at COPY time.
#
# We extract the fields we care about by key. AREA_TYPE in the source is a
# string code; cast to INT for typed downstream use.

OEWS_BRONZE_TO_SILVER_SQL = """
MERGE INTO SILVER.bls_oews_annual AS tgt
USING (
    SELECT
        survey_year || '|'
            || COALESCE(record:AREA::STRING, '')
            || '|'
            || COALESCE(record:OCC_CODE::STRING, '')        AS oews_key,
        survey_year,
        record:AREA::STRING                                 AS area,
        record:AREA_TITLE::STRING                           AS area_title,
        TRY_TO_NUMBER(record:AREA_TYPE::STRING)             AS area_type,
        record:OCC_CODE::STRING                             AS occ_code,
        record:OCC_TITLE::STRING                            AS occ_title,
        CASE
            WHEN record:O_GROUP::STRING ILIKE '%major%'    THEN 'major'
            WHEN record:O_GROUP::STRING ILIKE '%minor%'    THEN 'minor'
            WHEN record:O_GROUP::STRING ILIKE '%broad%'    THEN 'broad'
            WHEN record:O_GROUP::STRING ILIKE '%detailed%' THEN 'detailed'
            ELSE NULL
        END                                                 AS o_group,

        TRY_TO_DOUBLE(record:TOT_EMP::STRING)               AS tot_emp,
        TRY_TO_DOUBLE(record:A_MEAN::STRING)                AS a_mean,
        TRY_TO_DOUBLE(record:A_PCT10::STRING)               AS a_pct10,
        TRY_TO_DOUBLE(record:A_PCT25::STRING)               AS a_pct25,
        TRY_TO_DOUBLE(record:A_MEDIAN::STRING)              AS a_median,
        TRY_TO_DOUBLE(record:A_PCT75::STRING)               AS a_pct75,
        TRY_TO_DOUBLE(record:A_PCT90::STRING)               AS a_pct90,
        TRY_TO_DOUBLE(record:H_MEAN::STRING)                AS h_mean,
        TRY_TO_DOUBLE(record:H_MEDIAN::STRING)              AS h_median,

        MAX(ingestion_timestamp)                            AS ingestion_timestamp
    FROM BRONZE.raw_bls_oews
    WHERE record:OCC_CODE IS NOT NULL
      AND record:OCC_CODE::STRING NOT LIKE '00-%'           -- skip "All Occupations" totals
    GROUP BY ALL
) AS src
ON tgt.oews_key = src.oews_key
WHEN MATCHED AND src.ingestion_timestamp > tgt.ingestion_timestamp THEN UPDATE SET
    tot_emp = src.tot_emp, a_mean = src.a_mean,
    a_pct10 = src.a_pct10, a_pct25 = src.a_pct25, a_median = src.a_median,
    a_pct75 = src.a_pct75, a_pct90 = src.a_pct90,
    h_mean = src.h_mean, h_median = src.h_median,
    ingestion_timestamp = src.ingestion_timestamp
WHEN NOT MATCHED THEN INSERT (
    oews_key, survey_year, area, area_title, area_type,
    occ_code, occ_title, o_group,
    tot_emp, a_mean, a_pct10, a_pct25, a_median, a_pct75, a_pct90,
    h_mean, h_median, ingestion_timestamp
) VALUES (
    src.oews_key, src.survey_year, src.area, src.area_title, src.area_type,
    src.occ_code, src.occ_title, src.o_group,
    src.tot_emp, src.a_mean, src.a_pct10, src.a_pct25, src.a_median,
    src.a_pct75, src.a_pct90,
    src.h_mean, src.h_median, src.ingestion_timestamp
)
"""


# ---------------------------------------------------------------------------
# SOC resolver
# ---------------------------------------------------------------------------
# Approach:
#   1. Seed a small CTE with a curated subset of common job titles → SOC codes.
#      (See SOC_SEED_SQL below for the full list.) Each title there is the
#      "exact" anchor.
#   2. For every distinct title in jobs_unified, try to find an exact match
#      in the seed by normalized title. If found, match_method='exact'.
#   3. For titles that didn't match exactly, apply a fuzzy match using
#      Snowflake's EDITDISTANCE: the seed entry with the smallest edit
#      distance, provided that distance is below a threshold proportional
#      to title length, wins. match_method='fuzzy', confidence = 1 - dist/len.
#   4. Anything still unmatched gets the sentinel SOC '99-9999'
#      (BLS doesn't use this; we use it internally to mean "unmapped").

# ---- 3a. Seed lookup table (idempotent) ----
# We use a regular CTE inside the resolver instead of a permanent table
# because the seed is small (<300 rows) and we want to keep all the
# resolution logic in one inspectable query.

SOC_SEED_SQL = """
SELECT * FROM (VALUES
    -- Computer & Mathematical (15-XXXX)
    ('software engineer',        '15-1252', 'Software Developers'),
    ('software developer',       '15-1252', 'Software Developers'),
    ('software engineer intern', '15-1252', 'Software Developers'),
    ('full stack engineer',      '15-1254', 'Web Developers'),
    ('full stack developer',     '15-1254', 'Web Developers'),
    ('frontend engineer',        '15-1254', 'Web Developers'),
    ('frontend developer',       '15-1254', 'Web Developers'),
    ('backend engineer',         '15-1252', 'Software Developers'),
    ('backend developer',        '15-1252', 'Software Developers'),
    ('mobile developer',         '15-1252', 'Software Developers'),
    ('ios developer',            '15-1252', 'Software Developers'),
    ('android developer',        '15-1252', 'Software Developers'),
    ('web developer',            '15-1254', 'Web Developers'),
    ('data engineer',            '15-2051', 'Data Scientists'),
    ('data scientist',           '15-2051', 'Data Scientists'),
    ('machine learning engineer','15-2051', 'Data Scientists'),
    ('ml engineer',              '15-2051', 'Data Scientists'),
    ('data analyst',             '15-2041', 'Statisticians'),
    ('data analyst entry level', '15-2041', 'Statisticians'),
    ('business analyst',         '13-1111', 'Management Analysts'),
    ('business intelligence analyst','15-2041','Statisticians'),
    ('database administrator',   '15-1242', 'Database Administrators'),
    ('database engineer',        '15-1242', 'Database Administrators'),
    ('systems analyst',          '15-1211', 'Computer Systems Analysts'),
    ('systems administrator',    '15-1244', 'Network and Computer Systems Administrators'),
    ('network engineer',         '15-1241', 'Computer Network Architects'),
    ('network administrator',    '15-1244', 'Network and Computer Systems Administrators'),
    ('cloud engineer',           '15-1244', 'Network and Computer Systems Administrators'),
    ('devops engineer',          '15-1244', 'Network and Computer Systems Administrators'),
    ('site reliability engineer','15-1244', 'Network and Computer Systems Administrators'),
    ('security engineer',        '15-1212', 'Information Security Analysts'),
    ('cybersecurity analyst',    '15-1212', 'Information Security Analysts'),
    ('information security analyst','15-1212','Information Security Analysts'),
    ('qa engineer',              '15-1253', 'Software Quality Assurance Analysts and Testers'),
    ('quality assurance engineer','15-1253','Software Quality Assurance Analysts and Testers'),
    ('test engineer',            '15-1253', 'Software Quality Assurance Analysts and Testers'),
    ('product manager',          '11-3021', 'Computer and Information Systems Managers'),
    ('technical program manager','11-3021', 'Computer and Information Systems Managers'),
    ('it support specialist',    '15-1232', 'Computer User Support Specialists'),
    ('help desk analyst',        '15-1232', 'Computer User Support Specialists'),
    ('junior developer',         '15-1252', 'Software Developers'),
    ('new grad software',        '15-1252', 'Software Developers'),
    ('research scientist',       '15-2099', 'Mathematical Science Occupations, All Other'),
    ('statistician',             '15-2041', 'Statisticians'),

    -- Business & Financial (13-XXXX)
    ('accountant',               '13-2011', 'Accountants and Auditors'),
    ('auditor',                  '13-2011', 'Accountants and Auditors'),
    ('financial analyst',        '13-2051', 'Financial and Investment Analysts'),
    ('budget analyst',           '13-2031', 'Budget Analysts'),
    ('management analyst',       '13-1111', 'Management Analysts'),
    ('hr specialist',            '13-1071', 'Human Resources Specialists'),
    ('human resources specialist','13-1071','Human Resources Specialists'),
    ('marketing specialist',     '13-1161', 'Market Research Analysts and Marketing Specialists'),
    ('marketing manager',        '11-2021', 'Marketing Managers'),
    ('project manager',          '13-1082', 'Project Management Specialists'),

    -- Healthcare (29-XXXX, 31-XXXX)
    ('registered nurse',         '29-1141', 'Registered Nurses'),
    ('nurse practitioner',       '29-1171', 'Nurse Practitioners'),
    ('physician',                '29-1228', 'Physicians, All Other'),
    ('medical assistant',        '31-9092', 'Medical Assistants'),
    ('physical therapist',       '29-1123', 'Physical Therapists'),
    ('pharmacist',               '29-1051', 'Pharmacists'),
    ('dentist',                  '29-1021', 'Dentists, General'),

    -- Legal (23-XXXX)
    ('attorney',                 '23-1011', 'Lawyers'),
    ('lawyer',                   '23-1011', 'Lawyers'),
    ('paralegal',                '23-2011', 'Paralegals and Legal Assistants'),

    -- Education (25-XXXX)
    ('teacher',                  '25-2021', 'Elementary School Teachers'),
    ('elementary teacher',       '25-2021', 'Elementary School Teachers'),
    ('high school teacher',      '25-2031', 'Secondary School Teachers'),
    ('professor',                '25-1099', 'Postsecondary Teachers, All Other'),

    -- Sales & Office (41-XXXX, 43-XXXX)
    ('sales associate',          '41-2031', 'Retail Salespersons'),
    ('account executive',        '41-3091', 'Sales Representatives of Services'),
    ('customer service representative','43-4051','Customer Service Representatives'),
    ('administrative assistant', '43-6014', 'Secretaries and Administrative Assistants'),
    ('executive assistant',      '43-6011', 'Executive Secretaries and Executive Administrative Assistants'),

    -- Construction & Trades (47-XXXX, 49-XXXX)
    ('electrician',              '47-2111', 'Electricians'),
    ('plumber',                  '47-2152', 'Plumbers, Pipefitters, and Steamfitters'),
    ('carpenter',                '47-2031', 'Carpenters'),
    ('mechanic',                 '49-3023', 'Automotive Service Technicians and Mechanics'),

    -- Federal-flavored titles common in USAJobs
    ('information technology specialist','15-1212','Information Security Analysts'),
    ('it specialist',            '15-1212', 'Information Security Analysts'),
    ('contract specialist',      '13-1023', 'Purchasing Agents'),
    ('program analyst',          '13-1111', 'Management Analysts'),
    ('management and program analyst','13-1111','Management Analysts'),
    ('intelligence analyst',     '33-3021', 'Detectives and Criminal Investigators'),
    ('police officer',           '33-3051', 'Police and Sheriff Patrol Officers'),
    ('park ranger',              '33-3052', 'Transit and Railroad Police')
) AS t(title_norm, soc_code, soc_title)
"""


# Keyword-based fallback patterns. The resolver tries these AFTER exact and
# edit-distance fuzzy matching but BEFORE giving up. Each entry says: "if
# the normalized title contains any of these substrings, map to this SOC."
#
# Order matters — first matching pattern wins. We put more-specific patterns
# (e.g. 'machine learning') above more-general ones (e.g. 'engineer') so a
# Machine Learning Engineer doesn't get caught as a generic engineer.
#
# These patterns are evaluated by Snowflake's CONTAINS() in the resolver SQL.

SOC_KEYWORD_PATTERNS_SQL = """
SELECT * FROM (VALUES
    -- Specific tech roles BEFORE generic engineer/developer
    ('machine learning',         '15-2051', 'Data Scientists'),
    ('data science',             '15-2051', 'Data Scientists'),
    ('data scientist',           '15-2051', 'Data Scientists'),
    ('data engineer',            '15-2051', 'Data Scientists'),
    ('data analyst',             '15-2041', 'Statisticians'),
    ('database',                 '15-1242', 'Database Administrators'),
    ('cybersecurity',            '15-1212', 'Information Security Analysts'),
    ('cyber security',           '15-1212', 'Information Security Analysts'),
    ('information security',     '15-1212', 'Information Security Analysts'),
    ('information assurance',    '15-1212', 'Information Security Analysts'),
    ('network engineer',         '15-1241', 'Computer Network Architects'),
    ('network admin',            '15-1244', 'Network and Computer Systems Administrators'),
    ('system admin',             '15-1244', 'Network and Computer Systems Administrators'),
    ('systems admin',            '15-1244', 'Network and Computer Systems Administrators'),
    ('cloud',                    '15-1244', 'Network and Computer Systems Administrators'),
    ('devops',                   '15-1244', 'Network and Computer Systems Administrators'),
    ('site reliability',         '15-1244', 'Network and Computer Systems Administrators'),
    ('quality assurance',        '15-1253', 'Software Quality Assurance Analysts and Testers'),
    ('software quality',         '15-1253', 'Software Quality Assurance Analysts and Testers'),
    ('test engineer',            '15-1253', 'Software Quality Assurance Analysts and Testers'),
    ('qa engineer',              '15-1253', 'Software Quality Assurance Analysts and Testers'),
    ('web developer',            '15-1254', 'Web Developers'),
    ('frontend',                 '15-1254', 'Web Developers'),
    ('front end',                '15-1254', 'Web Developers'),
    ('front-end',                '15-1254', 'Web Developers'),
    ('full stack',               '15-1254', 'Web Developers'),
    ('full-stack',               '15-1254', 'Web Developers'),
    -- Generic developer/engineer/programmer last among tech
    ('software developer',       '15-1252', 'Software Developers'),
    ('software engineer',        '15-1252', 'Software Developers'),
    ('software',                 '15-1252', 'Software Developers'),
    ('developer',                '15-1252', 'Software Developers'),
    ('programmer',               '15-1251', 'Computer Programmers'),
    ('information technology',   '15-1212', 'Information Security Analysts'),
    ('it specialist',            '15-1212', 'Information Security Analysts'),

    -- USAJobs-style "Specialist" / "Analyst" / "Officer" patterns
    ('contract specialist',      '13-1023', 'Purchasing Agents'),
    ('contracting',              '13-1023', 'Purchasing Agents'),
    ('purchasing',               '13-1023', 'Purchasing Agents'),
    ('acquisition',              '13-1023', 'Purchasing Agents'),
    ('procurement',              '13-1023', 'Purchasing Agents'),
    ('logistics',                '13-1081', 'Logisticians'),
    ('supply chain',             '13-1081', 'Logisticians'),
    ('management analyst',       '13-1111', 'Management Analysts'),
    ('management and program',   '13-1111', 'Management Analysts'),
    ('program analyst',          '13-1111', 'Management Analysts'),
    ('policy analyst',           '13-1111', 'Management Analysts'),
    ('budget',                   '13-2031', 'Budget Analysts'),
    ('financial analyst',        '13-2051', 'Financial and Investment Analysts'),
    ('financial management',     '13-2051', 'Financial and Investment Analysts'),
    ('accountant',               '13-2011', 'Accountants and Auditors'),
    ('accounting',               '13-2011', 'Accountants and Auditors'),
    ('auditor',                  '13-2011', 'Accountants and Auditors'),
    ('audit',                    '13-2011', 'Accountants and Auditors'),
    ('human resources',          '13-1071', 'Human Resources Specialists'),
    ('hr specialist',            '13-1071', 'Human Resources Specialists'),
    ('personnel',                '13-1071', 'Human Resources Specialists'),
    ('equal employment',         '13-1041', 'Compliance Officers'),
    ('eeo',                      '13-1041', 'Compliance Officers'),
    ('compliance',               '13-1041', 'Compliance Officers'),
    ('marketing',                '13-1161', 'Market Research Analysts and Marketing Specialists'),
    ('public affairs',           '27-3031', 'Public Relations Specialists'),
    ('public relations',         '27-3031', 'Public Relations Specialists'),
    ('communications',           '27-3031', 'Public Relations Specialists'),
    ('writer',                   '27-3043', 'Writers and Authors'),
    ('editor',                   '27-3041', 'Editors'),
    ('translator',               '27-3091', 'Interpreters and Translators'),
    ('interpreter',              '27-3091', 'Interpreters and Translators'),
    ('project manager',          '13-1082', 'Project Management Specialists'),
    ('project management',       '13-1082', 'Project Management Specialists'),
    ('program manager',          '11-9199', 'Managers, All Other'),
    ('product manager',          '11-3021', 'Computer and Information Systems Managers'),

    -- Healthcare (29-XXXX, 31-XXXX) — heavy in USAJobs (VA hospitals)
    ('registered nurse',         '29-1141', 'Registered Nurses'),
    ('nurse practitioner',       '29-1171', 'Nurse Practitioners'),
    ('nursing assistant',        '31-1131', 'Nursing Assistants'),
    ('practical nurse',          '29-2061', 'Licensed Practical and Vocational Nurses'),
    ('nurse',                    '29-1141', 'Registered Nurses'),
    ('physician assistant',      '29-1071', 'Physician Assistants'),
    ('physician',                '29-1228', 'Physicians, All Other'),
    ('medical doctor',           '29-1228', 'Physicians, All Other'),
    ('psychiatrist',             '29-1223', 'Psychiatrists'),
    ('psychologist',             '19-3033', 'Clinical, Counseling, and School Psychologists'),
    ('social worker',            '21-1029', 'Social Workers, All Other'),
    ('counselor',                '21-1019', 'Counselors, All Other'),
    ('therapist',                '29-1129', 'Therapists, All Other'),
    ('occupational therapy',     '29-1122', 'Occupational Therapists'),
    ('physical therapy',         '29-1123', 'Physical Therapists'),
    ('respiratory therapy',      '29-1126', 'Respiratory Therapists'),
    ('speech',                   '29-1127', 'Speech-Language Pathologists'),
    ('audiologist',              '29-1181', 'Audiologists'),
    ('pharmacist',               '29-1051', 'Pharmacists'),
    ('pharmacy',                 '29-2052', 'Pharmacy Technicians'),
    ('dietitian',                '29-1031', 'Dietitians and Nutritionists'),
    ('nutritionist',             '29-1031', 'Dietitians and Nutritionists'),
    ('dental',                   '29-1021', 'Dentists, General'),
    ('optometrist',              '29-1041', 'Optometrists'),
    ('chiropractor',             '29-1011', 'Chiropractors'),
    ('podiatrist',               '29-1081', 'Podiatrists'),
    ('veterinarian',             '29-1131', 'Veterinarians'),
    ('clinical laboratory',      '29-2011', 'Medical and Clinical Laboratory Technologists'),
    ('medical technologist',     '29-2011', 'Medical and Clinical Laboratory Technologists'),
    ('medical technician',       '29-2012', 'Medical and Clinical Laboratory Technicians'),
    ('radiologic',               '29-2034', 'Radiologic Technologists and Technicians'),
    ('diagnostic',               '29-2032', 'Diagnostic Medical Sonographers'),
    ('biomedical equipment',     '49-9062', 'Medical Equipment Repairers'),
    ('medical equipment',        '49-9062', 'Medical Equipment Repairers'),
    ('health technician',        '29-2099', 'Health Technologists and Technicians, All Other'),
    ('health system',            '11-9111', 'Medical and Health Services Managers'),
    ('health services',          '11-9111', 'Medical and Health Services Managers'),
    ('healthcare',               '11-9111', 'Medical and Health Services Managers'),
    ('medical assistant',        '31-9092', 'Medical Assistants'),
    ('medical record',           '29-2071', 'Medical Records Specialists'),
    ('health information',       '29-2071', 'Medical Records Specialists'),
    ('public health',            '21-1094', 'Community Health Workers'),
    ('epidemiologist',           '19-1041', 'Epidemiologists'),
    ('biologist',                '19-1029', 'Biological Scientists, All Other'),
    ('chemist',                  '19-2031', 'Chemists'),
    ('microbiologist',           '19-1022', 'Microbiologists'),

    -- Engineering (17-XXXX) — common in USAJobs (DOD, NASA, etc.)
    ('aerospace engineer',       '17-2011', 'Aerospace Engineers'),
    ('aeronautical',             '17-2011', 'Aerospace Engineers'),
    ('astronaut',                '17-2011', 'Aerospace Engineers'),
    ('civil engineer',           '17-2051', 'Civil Engineers'),
    ('mechanical engineer',      '17-2141', 'Mechanical Engineers'),
    ('electrical engineer',      '17-2071', 'Electrical Engineers'),
    ('electronics engineer',     '17-2072', 'Electronics Engineers'),
    ('industrial engineer',      '17-2112', 'Industrial Engineers'),
    ('biomedical engineer',      '17-2031', 'Bioengineers and Biomedical Engineers'),
    ('chemical engineer',        '17-2041', 'Chemical Engineers'),
    ('environmental engineer',   '17-2081', 'Environmental Engineers'),
    ('nuclear engineer',         '17-2161', 'Nuclear Engineers'),
    ('petroleum engineer',       '17-2171', 'Petroleum Engineers'),
    ('engineer',                 '17-2199', 'Engineers, All Other'),

    -- Sciences (19-XXXX) — heavy in USAJobs (USGS, NOAA, EPA, NIH)
    ('research scientist',       '19-1042', 'Medical Scientists'),
    ('physicist',                '19-2012', 'Physicists'),
    ('geologist',                '19-2042', 'Geoscientists'),
    ('geoscientist',             '19-2042', 'Geoscientists'),
    ('hydrologist',              '19-2043', 'Hydrologists'),
    ('meteorologist',            '19-2021', 'Atmospheric and Space Scientists'),
    ('atmospheric',              '19-2021', 'Atmospheric and Space Scientists'),
    ('environmental scientist',  '19-2041', 'Environmental Scientists and Specialists'),
    ('soil scientist',           '19-1013', 'Soil and Plant Scientists'),
    ('forester',                 '19-1032', 'Foresters'),
    ('forestry',                 '19-1032', 'Foresters'),
    ('wildlife',                 '19-1023', 'Zoologists and Wildlife Biologists'),
    ('fisheries',                '19-1023', 'Zoologists and Wildlife Biologists'),
    ('archaeologist',            '19-3091', 'Anthropologists and Archeologists'),
    ('historian',                '19-3093', 'Historians'),
    ('economist',                '19-3011', 'Economists'),
    ('statistician',             '15-2041', 'Statisticians'),
    ('mathematician',            '15-2021', 'Mathematicians'),
    ('actuary',                  '15-2011', 'Actuaries'),
    ('analyst',                  '13-1111', 'Management Analysts'),     -- catch-all for "X Analyst"
    ('scientist',                '19-1029', 'Biological Scientists, All Other'),

    -- Protective service (33-XXXX) — DOD/DHS/VA police, security
    ('police officer',           '33-3051', 'Police and Sheriff Patrol Officers'),
    ('police',                   '33-3051', 'Police and Sheriff Patrol Officers'),
    ('detective',                '33-3021', 'Detectives and Criminal Investigators'),
    ('criminal investigator',    '33-3021', 'Detectives and Criminal Investigators'),
    ('special agent',            '33-3021', 'Detectives and Criminal Investigators'),
    ('intelligence',             '33-3021', 'Detectives and Criminal Investigators'),
    ('targeting analyst',        '33-3021', 'Detectives and Criminal Investigators'),
    ('counterintelligence',      '33-3021', 'Detectives and Criminal Investigators'),
    ('correctional',             '33-3012', 'Correctional Officers and Jailers'),
    ('security officer',         '33-9032', 'Security Guards'),
    ('security guard',           '33-9032', 'Security Guards'),
    ('firefighter',              '33-2011', 'Firefighters'),
    ('fire',                     '33-2011', 'Firefighters'),
    ('park ranger',              '33-9092', 'Lifeguards, Ski Patrol, and Other Recreational Protective Service Workers'),
    ('border patrol',            '33-3051', 'Police and Sheriff Patrol Officers'),
    ('customs',                  '33-3051', 'Police and Sheriff Patrol Officers'),
    ('air traffic',              '53-2021', 'Air Traffic Controllers'),

    -- Education (25-XXXX)
    ('teacher',                  '25-2021', 'Elementary School Teachers'),
    ('professor',                '25-1099', 'Postsecondary Teachers, All Other'),
    ('instructor',               '25-1099', 'Postsecondary Teachers, All Other'),
    ('librarian',                '25-4022', 'Librarians and Media Collections Specialists'),
    ('education',                '25-9099', 'Educational Instruction and Library Workers, All Other'),

    -- Legal (23-XXXX)
    ('attorney',                 '23-1011', 'Lawyers'),
    ('lawyer',                   '23-1011', 'Lawyers'),
    ('counsel',                  '23-1011', 'Lawyers'),
    ('judge',                    '23-1023', 'Judges'),
    ('paralegal',                '23-2011', 'Paralegals and Legal Assistants'),
    ('legal',                    '23-2011', 'Paralegals and Legal Assistants'),

    -- Trades & maintenance (47-XXXX, 49-XXXX, 51-XXXX, 53-XXXX)
    ('electrician',              '47-2111', 'Electricians'),
    ('plumber',                  '47-2152', 'Plumbers, Pipefitters, and Steamfitters'),
    ('carpenter',                '47-2031', 'Carpenters'),
    ('mechanic',                 '49-3023', 'Automotive Service Technicians and Mechanics'),
    ('maintenance',              '49-9071', 'Maintenance and Repair Workers, General'),
    ('truck driver',             '53-3032', 'Heavy and Tractor-Trailer Truck Drivers'),
    ('driver',                   '53-3032', 'Heavy and Tractor-Trailer Truck Drivers'),
    ('aircraft mechanic',        '49-3011', 'Aircraft Mechanics and Service Technicians'),
    ('pilot',                    '53-2011', 'Airline Pilots, Copilots, and Flight Engineers'),
    ('captain',                  '53-5021', 'Captains, Mates, and Pilots of Water Vessels'),
    ('warehouse',                '53-7065', 'Stockers and Order Fillers'),

    -- Office & support (43-XXXX)
    ('administrative assistant', '43-6014', 'Secretaries and Administrative Assistants'),
    ('executive assistant',      '43-6011', 'Executive Secretaries and Executive Administrative Assistants'),
    ('secretary',                '43-6014', 'Secretaries and Administrative Assistants'),
    ('clerk',                    '43-9061', 'Office Clerks, General'),
    ('receptionist',             '43-4171', 'Receptionists and Information Clerks'),
    ('customer service',         '43-4051', 'Customer Service Representatives'),
    ('call center',              '43-4051', 'Customer Service Representatives'),
    ('data entry',               '43-9021', 'Data Entry Keyers'),

    -- Management (11-XXXX) — "Supervisory X" patterns common in USAJobs
    ('supervisory',              '11-9199', 'Managers, All Other'),
    ('director',                 '11-9199', 'Managers, All Other'),
    ('chief',                    '11-1011', 'Chief Executives'),
    ('manager',                  '11-9199', 'Managers, All Other'),
    ('specialist',               '13-1199', 'Business Operations Specialists, All Other')
        -- 'specialist' is the absolute last fallback — many USAJobs titles end in it
) AS t(pattern, soc_code, soc_title)
"""


# ---- 3b. Resolver MERGE ----
# Single statement that:
#   - normalizes every distinct title in jobs_unified
#   - tries exact match against the seed
#   - falls back to fuzzy match (best edit-distance below threshold)
#   - inserts/updates dim_occupation_soc with the result
#
# The fuzzy threshold scales with title length (allow ~15% character churn).

RESOLVE_SOC_SQL = f"""
MERGE INTO SILVER.dim_occupation_soc AS tgt
USING (
    WITH seed AS ({SOC_SEED_SQL}),
    keyword_patterns AS ({SOC_KEYWORD_PATTERNS_SQL}),

    distinct_titles AS (
        SELECT
            -- Normalize: lowercase, strip punctuation, collapse whitespace
            TRIM(REGEXP_REPLACE(LOWER(job_title), '[^a-z0-9 ]+', ' '))
                                                            AS title_norm,
            ANY_VALUE(job_title)                            AS title_raw,
            COUNT(*)                                        AS posting_count
        FROM SILVER.jobs_unified
        WHERE job_title IS NOT NULL
        GROUP BY 1
    ),

    exact_matches AS (
        SELECT
            d.title_norm,
            d.title_raw,
            d.posting_count,
            s.soc_code,
            s.soc_title,
            'exact'      AS match_method,
            1.0          AS match_confidence
        FROM distinct_titles d
        JOIN seed s
          ON d.title_norm = s.title_norm
    ),

    -- Best fuzzy match (edit distance) for everything that didn't match exactly.
    fuzzy_candidates AS (
        SELECT
            d.title_norm,
            d.title_raw,
            d.posting_count,
            s.soc_code,
            s.soc_title,
            EDITDISTANCE(d.title_norm, s.title_norm)        AS dist,
            LENGTH(d.title_norm)                            AS len_norm,
            ROW_NUMBER() OVER (
                PARTITION BY d.title_norm
                ORDER BY EDITDISTANCE(d.title_norm, s.title_norm)
            )                                               AS rn
        FROM distinct_titles d
        CROSS JOIN seed s
        WHERE d.title_norm NOT IN (SELECT title_norm FROM exact_matches)
          AND LENGTH(d.title_norm) BETWEEN 4 AND 60
    ),
    fuzzy_matches AS (
        SELECT
            title_norm, title_raw, posting_count,
            soc_code, soc_title,
            'fuzzy'                                         AS match_method,
            1.0 - (dist::FLOAT / NULLIF(len_norm, 0))       AS match_confidence
        FROM fuzzy_candidates
        WHERE rn = 1
          AND dist <= GREATEST(2, ROUND(len_norm * 0.15))
    ),

    -- Keyword-substring fallback. For titles that exact + fuzzy missed,
    -- find the FIRST keyword pattern that appears in the normalized title.
    -- ROW_NUMBER over a CASE-WHEN ranks patterns in the order they appear
    -- in keyword_patterns table (more specific first; see SOC_KEYWORD_PATTERNS_SQL).
    keyword_candidates AS (
        SELECT
            d.title_norm,
            d.title_raw,
            d.posting_count,
            k.soc_code,
            k.soc_title,
            k.pattern,
            -- Stable ordering of patterns: use a deterministic surrogate.
            -- We re-derive pattern priority by looking up its row position
            -- via a self-join trick: shorter patterns rank LATER (catch-all
            -- single words like 'engineer' come after 'aerospace engineer').
            ROW_NUMBER() OVER (
                PARTITION BY d.title_norm
                ORDER BY LENGTH(k.pattern) DESC, k.pattern
            )                                               AS rn
        FROM distinct_titles d
        CROSS JOIN keyword_patterns k
        WHERE d.title_norm NOT IN (SELECT title_norm FROM exact_matches)
          AND d.title_norm NOT IN (SELECT title_norm FROM fuzzy_matches)
          AND CONTAINS(d.title_norm, k.pattern)
    ),
    keyword_matches AS (
        SELECT
            title_norm, title_raw, posting_count,
            soc_code, soc_title,
            'keyword'                                       AS match_method,
            -- Confidence reflects how much of the title the matched pattern covers.
            LEAST(1.0, LENGTH(pattern)::FLOAT / NULLIF(LENGTH(title_norm), 0))
                                                            AS match_confidence
        FROM keyword_candidates
        WHERE rn = 1
    ),

    unmapped AS (
        SELECT
            d.title_norm,
            d.title_raw,
            d.posting_count,
            '99-9999'    AS soc_code,
            'Unmapped'   AS soc_title,
            'unmapped'   AS match_method,
            0.0          AS match_confidence
        FROM distinct_titles d
        WHERE d.title_norm NOT IN (SELECT title_norm FROM exact_matches)
          AND d.title_norm NOT IN (SELECT title_norm FROM fuzzy_matches)
          AND d.title_norm NOT IN (SELECT title_norm FROM keyword_matches)
    ),

    all_resolutions AS (
        SELECT * FROM exact_matches
        UNION ALL SELECT * FROM fuzzy_matches
        UNION ALL SELECT * FROM keyword_matches
        UNION ALL SELECT * FROM unmapped
    )

    SELECT
        soc_code,
        soc_title,
        LEFT(soc_code, 2) || '-0000'                        AS soc_major_group,
        title_norm                                          AS title_normalized,
        title_raw,
        match_method,
        match_confidence,
        posting_count,
        CURRENT_TIMESTAMP()                                 AS last_resolved_at
    FROM all_resolutions
) AS src
ON tgt.title_normalized = src.title_normalized
WHEN MATCHED THEN UPDATE SET
    soc_code            = src.soc_code,
    soc_title           = src.soc_title,
    soc_major_group     = src.soc_major_group,
    title_raw           = src.title_raw,
    match_method        = src.match_method,
    match_confidence    = src.match_confidence,
    posting_count       = src.posting_count,
    last_resolved_at    = src.last_resolved_at
WHEN NOT MATCHED THEN INSERT (
    soc_code, soc_title, soc_major_group,
    title_normalized, title_raw,
    match_method, match_confidence, posting_count, last_resolved_at
) VALUES (
    src.soc_code, src.soc_title, src.soc_major_group,
    src.title_normalized, src.title_raw,
    src.match_method, src.match_confidence, src.posting_count, src.last_resolved_at
)
"""


# Friendly stats query used by the DAG for observability.
SOC_STATS_SQL = """
SELECT match_method, COUNT(*) AS title_count, SUM(posting_count) AS total_postings
FROM SILVER.dim_occupation_soc
GROUP BY match_method
ORDER BY match_method
"""


# ---------------------------------------------------------------------------
# Class wrapper
# ---------------------------------------------------------------------------

class BLSBronzeToSilverTransformer:
    def __init__(self) -> None:
        require("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")

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

    def _execute(self, sql: str, label: str) -> int:
        logger.info("Running MERGE: %s", label)
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                affected = cur.rowcount or 0
                conn.commit()
            finally:
                cur.close()
        logger.info("%s affected %d rows", label, affected)
        return affected

    def transform_qcew(self) -> int:
        return self._execute(QCEW_BRONZE_TO_SILVER_SQL, "qcew bronze->silver")

    def transform_oews(self) -> int:
        return self._execute(OEWS_BRONZE_TO_SILVER_SQL, "oews bronze->silver")

    def resolve_soc(self) -> int:
        rows = self._execute(RESOLVE_SOC_SQL, "soc resolver")
        # Log the breakdown so the Airflow log makes the resolver result legible.
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(SOC_STATS_SQL)
                for method, n_titles, n_postings in cur.fetchall():
                    logger.info(
                        "SOC resolver — %s: %s distinct titles, %s postings",
                        method, f"{n_titles:,}", f"{n_postings:,}",
                    )
            finally:
                cur.close()
        return rows
