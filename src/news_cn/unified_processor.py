"""
Unified GDELT Processor
Single processor with pluggable strategies for different processing modes
Replaces: processor.py, batch_processor.py, streaming_processor.py, efficient_processor.py
"""

import logging
import zipfile
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import requests

from .config import Config
from .duckdb_utils import DuckDBConfig, DuckDBConnectionManager, DuckDBQueryBuilder
from .partition_utils import (
    ensure_partition_dir,
    get_consolidated_filename,
    group_files_by_date,
)
from .schemas import SchemaFactory

logger = logging.getLogger(__name__)


class ProcessingStrategy(ABC):
    """Abstract base class for processing strategies"""

    @abstractmethod
    def process_day(
        self,
        urls: list[str],
        date_str: str,
        output_dir: Path,
        target_country: str,
        config: Config,
    ) -> Path | None:
        """
        Process all files for one day

        Args:
            urls: List of file URLs for the day
            date_str: Date string in YYYYMMDD format
            output_dir: Base output directory
            target_country: Country code to filter
            config: Pipeline configuration

        Returns:
            Path to output parquet file or None if failed
        """
        pass


class BatchStrategy(ProcessingStrategy):
    """
    Batch processing strategy
    Downloads all files in parallel, then processes with single DuckDB query
    """

    def _download_and_extract(
        self, file_url: str, temp_dir: Path, timeout: int
    ) -> tuple[str, Path | None]:
        """Download and extract a single ZIP file"""
        filename = Path(file_url).name
        try:
            response = requests.get(file_url, timeout=timeout)
            response.raise_for_status()

            with zipfile.ZipFile(BytesIO(response.content), "r") as zip_ref:
                csv_filename = zip_ref.namelist()[0]
                csv_path = temp_dir / csv_filename
                zip_ref.extract(csv_filename, temp_dir)

            logger.debug(f"✓ Downloaded and extracted: {filename}")
            return (file_url, csv_path)

        except Exception as e:
            logger.error(f"✗ Failed {filename}: {e}")
            return (file_url, None)

    def _download_parallel(self, urls: list[str], temp_dir: Path, config: Config) -> list[Path]:
        """Download and extract all files in parallel"""
        logger.info(f"Downloading {len(urls)} files with {config.DOWNLOAD_WORKERS} workers...")

        extracted_paths = []

        with ThreadPoolExecutor(max_workers=config.DOWNLOAD_WORKERS) as executor:
            future_to_url = {
                executor.submit(
                    self._download_and_extract, url, temp_dir, config.DOWNLOAD_TIMEOUT
                ): url
                for url in urls
            }

            completed = 0
            for completed, future in enumerate(as_completed(future_to_url), start=1):
                _url, csv_path = future.result()

                if csv_path:
                    extracted_paths.append(csv_path)

                if completed % 10 == 0:
                    logger.info(
                        f"Progress: {completed}/{len(urls)} downloaded "
                        f"({len(extracted_paths)} successful)"
                    )

        logger.info(f"✓ Download complete: {len(extracted_paths)}/{len(urls)} files extracted")
        return extracted_paths

    def process_day(
        self,
        urls: list[str],
        date_str: str,
        output_dir: Path,
        target_country: str,
        config: Config,
    ) -> Path | None:
        """Process day using batch strategy"""
        if not urls:
            logger.warning(f"No URLs provided for {date_str}")
            return None

        logger.info(f"Processing {date_str} with batch strategy ({len(urls)} files)")

        # Get schema
        schema = SchemaFactory.get_event_schema()
        essential_columns = SchemaFactory.get_column_names("export", essential_only=True)

        # Setup output
        file_date = datetime.strptime(date_str, "%Y%m%d")
        partition_dir = ensure_partition_dir(output_dir, file_date, "export")
        output_file = partition_dir / get_consolidated_filename(date_str)

        # Skip if already processed
        if output_file.exists():
            logger.info(f"Already processed: {output_file.name}")
            return output_file

        # Use temporary directory for CSVs
        with TemporaryDirectory(prefix=f"gdelt_{date_str}_") as temp_dir:
            temp_path = Path(temp_dir)

            # Step 1: Parallel download and extraction
            csv_paths = self._download_parallel(urls, temp_path, config)

            if not csv_paths:
                logger.error(f"No files successfully downloaded for {date_str}")
                return None

            # Step 2: Batch processing with DuckDB
            try:
                csv_pattern = str(temp_path / "*.export.CSV")

                # Setup DuckDB
                duckdb_config = DuckDBConfig(
                    memory_limit=config.DUCKDB_MEMORY_LIMIT,
                    threads=config.DUCKDB_THREADS,
                    compression=config.DUCKDB_COMPRESSION,
                    temp_directory=config.DUCKDB_TEMP_DIRECTORY,
                )

                with DuckDBConnectionManager(duckdb_config) as conn:
                    # Build and execute query
                    query_builder = (
                        DuckDBQueryBuilder(duckdb_config)
                        .select(essential_columns)
                        .from_csv(csv_pattern, schema.to_duckdb_dict())
                        .where_country(target_country)
                    )

                    if config.NORMALIZE_COUNTRY_CODES:
                        query_builder.with_country_normalization()

                    query_builder.to_parquet(str(output_file))
                    query_builder.execute(conn)

                    # Verify output
                    result = conn.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{output_file}')"
                    ).fetchone()
                    record_count = result[0] if result else 0

                    if record_count > 0:
                        logger.info(
                            f"✓ Successfully processed {date_str}: {record_count:,} records"
                        )
                        return output_file
                    else:
                        logger.warning(f"No records found for {date_str}")
                        output_file.unlink()
                        return None

            except Exception as e:
                logger.error(f"Failed to process {date_str}: {e}")
                if output_file.exists():
                    output_file.unlink()
                return None


class StreamingStrategy(ProcessingStrategy):
    """
    Streaming strategy
    Processes files one at a time without storing locally (memory efficient)
    """

    def process_day(
        self,
        urls: list[str],
        date_str: str,
        output_dir: Path,
        target_country: str,
        config: Config,
    ) -> Path | None:
        """Process day using streaming strategy"""
        logger.info(f"Processing {date_str} with streaming strategy ({len(urls)} files)")
        # TODO: Implement streaming via DuckDB httpfs
        # For now, delegate to batch strategy
        return BatchStrategy().process_day(urls, date_str, output_dir, target_country, config)


class GDELTProcessor:
    """
    Unified GDELT Processor with pluggable strategies

    Usage:
        # Default (batch processing)
        processor = GDELTProcessor()

        # Specify strategy
        processor = GDELTProcessor(strategy="batch", config=my_config)

        # Process files
        results = processor.process_all_days(file_list, target_country="SA")
    """

    def __init__(
        self,
        strategy: str = "batch",
        config: Config | None = None,
        output_dir: str | None = None,
    ):
        """
        Initialize processor

        Args:
            strategy: Processing strategy ("batch" or "streaming")
            config: Configuration object (uses defaults if None)
            output_dir: Output directory (overrides config if provided)
        """
        self.config = config or Config()
        self.output_dir = Path(output_dir) if output_dir else self.config.PARQUET_OUTPUT_DIR

        # Select strategy
        strategies = {
            "batch": BatchStrategy(),
            "streaming": StreamingStrategy(),
        }

        if strategy not in strategies:
            raise ValueError(
                f"Unknown strategy: {strategy}. Choose from: {list(strategies.keys())}"
            )

        self.strategy = strategies[strategy]
        logger.info(f"Initialized GDELTProcessor with '{strategy}' strategy")

    def process_day(
        self, urls: list[str], date_str: str, target_country: str | None = None
    ) -> Path | None:
        """
        Process all files for one day

        Args:
            urls: List of file URLs for the day
            date_str: Date string in YYYYMMDD format
            target_country: Country code to filter (uses config default if None)

        Returns:
            Path to output parquet file
        """
        target = target_country or self.config.TARGET_COUNTRY_CODE
        return self.strategy.process_day(urls, date_str, self.output_dir, target, self.config)

    def process_all_days(
        self,
        file_list: list[tuple[str, str]],
        target_country: str | None = None,
    ) -> dict[str, Path]:
        """
        Process all days in the file list

        Args:
            file_list: List of tuples (file_size, file_url) from downloader
            target_country: Country code to filter (uses config default if None)

        Returns:
            Dictionary mapping date_str -> output path
        """
        grouped_urls = group_files_by_date(file_list)
        results = {}

        target = target_country or self.config.TARGET_COUNTRY_CODE
        logger.info(f"Processing {len(grouped_urls)} days with {len(file_list)} total files")

        for idx, (date_str, urls) in enumerate(sorted(grouped_urls.items()), 1):
            logger.info(f"\n[Day {idx}/{len(grouped_urls)}] {date_str}")

            output_path = self.process_day(urls, date_str, target)
            if output_path:
                results[date_str] = output_path

        logger.info(
            f"\n{'=' * 70}\n"
            f"✅ Pipeline complete: {len(results)}/{len(grouped_urls)} days processed\n"
            f"{'=' * 70}"
        )
        return results
