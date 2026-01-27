"""
Configuration Module
Centralized configuration using dataclasses (modern Python)
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class Config:
    """Pipeline configuration"""

    # GDELT URLs
    GDELT_V2_MASTER_FILE: str = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    GDELT_V2_LAST_UPDATE: str = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

    # Target country
    TARGET_COUNTRY_CODE: str = "SA"
    TARGET_COUNTRY_NAME: str = "Saudi Arabia"

    # Date filtering
    START_DATE: datetime = field(default_factory=lambda: datetime(2026, 1, 1))

    # Data types to process
    DATA_TYPES: list[str] = field(default_factory=lambda: ["export"])  # Start with events only

    # Directory configuration
    RAW_DATA_DIR: Path = field(default_factory=lambda: Path("data/raw"))
    PARQUET_OUTPUT_DIR: Path = field(default_factory=lambda: Path("data/parquet"))

    # Processing configuration
    CHUNK_SIZE: int = 100000
    DELETE_RAW_AFTER_PROCESSING: bool = True  # Auto-cleanup raw files
    AUTO_CONSOLIDATE: bool = True  # Auto-consolidate completed days
    CLEAN_RAW_DIR_ON_START: bool = True  # Clean raw directory before each run

    # DuckDB configuration
    DUCKDB_MEMORY_LIMIT: str = "4GB"
    DUCKDB_THREADS: int = 4
    DUCKDB_COMPRESSION: str = "ZSTD"  # Parquet compression algorithm
    DUCKDB_ENABLE_PROGRESS_BAR: bool = True
    DUCKDB_TEMP_DIRECTORY: str | None = None

    # Download configuration
    DOWNLOAD_WORKERS: int = 10  # Parallel download workers
    DOWNLOAD_TIMEOUT: int = 60  # Request timeout in seconds
    DOWNLOAD_CHUNK_SIZE: int = 8192  # Chunk size for streaming downloads

    # Data cleaning configuration
    DROP_LOW_VALUE_COLUMNS: bool = True  # Drop columns with 99%+ NULL rate
    FILTER_HISTORICAL_DATES: bool = False  # Keep retrospective articles
    NORMALIZE_COUNTRY_CODES: bool = True  # Convert 3-letter to 2-letter codes

    # Processing strategy
    PROCESSOR_STRATEGY: str = "batch"  # "batch", "streaming", or "efficient"

    def __post_init__(self):
        """Ensure directories exist"""
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# GDELT Column definitions (for reference)
EVENT_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]

MENTIONS_COLUMNS = [
    "GLOBALEVENTID",
    "EventTimeDate",
    "MentionTimeDate",
    "MentionType",
    "MentionSourceName",
    "MentionIdentifier",
    "SentenceID",
    "Actor1CharOffset",
    "Actor2CharOffset",
    "ActionCharOffset",
    "InRawText",
    "Confidence",
    "MentionDocLen",
    "MentionDocTone",
    "MentionDocTranslationInfo",
    "Extras",
]

# Essential columns (drops 99%+ NULL columns for better performance)
EVENT_COLUMNS_ESSENTIAL = [
    # Core event identification
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    # Actor 1 (essential only)
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1Type1Code",  # Primary type (72% NULL - keep for when present)
    # Actor 2 (essential only)
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2Type1Code",
    # Event classification
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    # Event metrics
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    # Actor 1 Geography
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    # Actor 2 Geography
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    # Action Geography
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    # Source
    "DATEADDED",
    "SOURCEURL",
]

# Country code mapping (3-letter to 2-letter ISO)
COUNTRY_CODE_MAP = {
    "SAU": "SA",  # Saudi Arabia
    "ARE": "AE",  # UAE
    "QAT": "QA",  # Qatar
    "KWT": "KW",  # Kuwait
    "BHR": "BH",  # Bahrain
    "OMN": "OM",  # Oman
    "YEM": "YE",  # Yemen
    "IRQ": "IQ",  # Iraq
    "JOR": "JO",  # Jordan
    "LBN": "LB",  # Lebanon
    "SYR": "SY",  # Syria
    "ISR": "IL",  # Israel
    "PSE": "PS",  # Palestine
    "EGY": "EG",  # Egypt
    "TUR": "TR",  # Turkey
    "IRN": "IR",  # Iran
    "PAK": "PK",  # Pakistan
    "IND": "IN",  # India
    "USA": "US",  # United States
    "GBR": "GB",  # United Kingdom
    "FRA": "FR",  # France
    "DEU": "DE",  # Germany
    "CHN": "CN",  # China
    "RUS": "RU",  # Russia
    "JPN": "JP",  # Japan
}
