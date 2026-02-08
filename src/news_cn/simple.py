"""
Simple API for GDELT News Collection
Beginner-friendly interface with sensible defaults
"""

import logging
from datetime import datetime
from pathlib import Path

from .config import Config
from .downloader import GDELTDownloader
from .unified_processor import GDELTProcessor

logger = logging.getLogger(__name__)


def collect_news(
    country: str = "SA",
    start_date: datetime | str | None = None,
    end_date: datetime | str | None = None,
    data_types: list[str] | None = None,
    output_dir: str = "data/parquet",
    strategy: str = "batch",
) -> dict[str, Path]:
    """
    Simple one-line function to collect GDELT news data

    Args:
        country: 2-letter country code (default: "SA" for Saudi Arabia)
        start_date: Start date for collection (default: 2026-01-01)
                   Can be datetime object or string like "2026-01-15"
        data_types: Types of data to collect (default: ["export"] for events)
        output_dir: Where to save data (default: "data/parquet")
        strategy: Processing strategy - "batch" (fast) or "streaming" (memory efficient)

    Returns:
        Dictionary mapping date -> output file path

    Example:
        >>> # Collect all Saudi Arabia news from Jan 1, 2026
        >>> results = collect_news()

        >>> # Collect UAE news from specific date
        >>> results = collect_news(country="AE", start_date="2026-01-15")

        >>> # Use streaming for low memory
        >>> results = collect_news(strategy="streaming")
    """
    # Setup configuration
    config = Config()
    config.TARGET_COUNTRY_CODE = country

    # Parse start date
    if start_date is None:
        start_date = datetime(2026, 1, 1)
    elif isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")

    config.START_DATE = start_date

    # Parse end date
    if end_date is not None and isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Set data types
    if data_types:
        config.DATA_TYPES = data_types

    # Initialize components
    downloader = GDELTDownloader(raw_data_dir=str(config.RAW_DATA_DIR))
    processor = GDELTProcessor(strategy=strategy, config=config, output_dir=output_dir)

    # Get available files
    logger.info(f"Fetching GDELT files from {start_date.date()}...")
    file_list = downloader.get_available_files(
        start_date=start_date, end_date=end_date, data_types=config.DATA_TYPES
    )

    if not file_list:
        logger.warning("No files found for specified criteria")
        return {}

    logger.info(f"Found {len(file_list)} files to process")

    # Process files
    results = processor.process_all_days(file_list, target_country=country)

    logger.info(f"✅ Collection complete: {len(results)} days processed")
    return results


def query_news(
    country: str = "SA",
    limit: int = 10,
    data_dir: str = "data/parquet",
) -> list[dict]:
    """
    Quick query to see recent news events

    Args:
        country: Country code to filter
        limit: Number of events to return
        data_dir: Where parquet files are stored

    Returns:
        List of event dictionaries

    Example:
        >>> # Get 10 most recent events
        >>> events = query_news()
        >>> for event in events:
        ...     print(f"{event['SQLDATE']}: {event['Actor1Name']} -> {event['Actor2Name']}")
    """
    import duckdb

    pattern = f"{data_dir}/events/**/*.parquet"

    conn = duckdb.connect(":memory:")
    query = f"""
        SELECT
            SQLDATE,
            Actor1Name,
            Actor2Name,
            EventCode,
            GoldsteinScale,
            AvgTone,
            ActionGeo_FullName,
            SOURCEURL
        FROM read_parquet('{pattern}')
        WHERE ActionGeo_CountryCode = '{country}'
        ORDER BY SQLDATE DESC, DATEADDED DESC
        LIMIT {limit}
    """

    results = conn.execute(query).fetchall()
    conn.close()

    # Convert to list of dicts
    columns = [
        "SQLDATE",
        "Actor1Name",
        "Actor2Name",
        "EventCode",
        "GoldsteinScale",
        "AvgTone",
        "Location",
        "URL",
    ]

    return [dict(zip(columns, row, strict=False)) for row in results]


class SimplePipeline:
    """
    Fluent API for building GDELT pipelines

    Example:
        >>> pipeline = (SimplePipeline()
        ...     .for_country("SA")
        ...     .from_date("2026-01-01")
        ...     .to_date("2026-01-31")
        ...     .use_batch_processing()
        ...     .run())

        >>> # Check results
        >>> print(f"Processed {len(pipeline.results)} days")

        >>> # Query the data
        >>> events = pipeline.query(limit=20)
    """

    def __init__(self):
        self.config = Config()
        self._country = "SA"
        self._start_date = datetime(2026, 1, 1)
        self._end_date = None
        self._strategy = "batch"
        self._output_dir = "data/parquet"
        self.results: dict[str, Path] = {}

    def for_country(self, country: str) -> "SimplePipeline":
        """Set target country (2-letter code)"""
        self._country = country
        return self

    def from_date(self, date: str | datetime) -> "SimplePipeline":
        """Set start date"""
        if isinstance(date, str):
            self._start_date = datetime.strptime(date, "%Y-%m-%d")
        else:
            self._start_date = date
        return self

    def to_date(self, date: str | datetime) -> "SimplePipeline":
        """Set end date (optional)"""
        if isinstance(date, str):
            self._end_date = datetime.strptime(date, "%Y-%m-%d")
        else:
            self._end_date = date
        return self

    def use_batch_processing(self) -> "SimplePipeline":
        """Use batch processing (fast, more memory)"""
        self._strategy = "batch"
        return self

    def use_streaming(self) -> "SimplePipeline":
        """Use streaming (slower, less memory)"""
        self._strategy = "streaming"
        return self

    def output_to(self, directory: str) -> "SimplePipeline":
        """Set output directory"""
        self._output_dir = directory
        return self

    def run(self) -> "SimplePipeline":
        """Execute the pipeline"""
        self.results = collect_news(
            country=self._country,
            start_date=self._start_date,
            output_dir=self._output_dir,
            strategy=self._strategy,
        )
        return self

    def query(self, limit: int = 10) -> list[dict]:
        """Query collected data"""
        return query_news(
            country=self._country,
            limit=limit,
            data_dir=self._output_dir,
        )


# Convenience shortcuts
def collect_saudi_news(days_back: int = 7) -> dict[str, Path]:
    """
    Quick shortcut to collect recent Saudi news

    Args:
        days_back: How many days back to collect (default: 7)

    Returns:
        Dictionary mapping date -> output file

    Example:
        >>> # Get last week of Saudi news
        >>> results = collect_saudi_news(days_back=7)
    """
    from datetime import timedelta

    start_date = datetime.now() - timedelta(days=days_back)
    return collect_news(country="SA", start_date=start_date)


def collect_uae_news(days_back: int = 7) -> dict[str, Path]:
    """Quick shortcut for UAE news"""
    from datetime import timedelta

    start_date = datetime.now() - timedelta(days=days_back)
    return collect_news(country="AE", start_date=start_date)
