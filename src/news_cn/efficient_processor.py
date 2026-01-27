"""
Efficient GDELT Processor
Streams and filters data without storing full raw files
100% FREE - No API keys or external services required
"""

import io
import logging
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import duckdb
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EfficientGDELTProcessor:
    """
    Efficiently processes GDELT data by streaming and filtering in one pass.
    No need to store massive raw files!
    """

    def __init__(
        self, output_dir: str = "data/parquet", memory_limit: str = "4GB", threads: int = 4
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.memory_limit = memory_limit
        self.threads = threads
        self.conn = self._init_duckdb()

    def _init_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Initialize DuckDB connection with optimal settings"""
        conn = duckdb.connect(":memory:")
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        conn.execute(f"SET threads={self.threads}")
        conn.execute("SET enable_progress_bar=true")
        logger.info(
            f"Initialized DuckDB with {self.memory_limit} memory and {self.threads} threads"
        )
        return conn

    def stream_and_filter_events(self, file_url: str, target_country: str = "SA") -> Path | None:
        """
        Stream GDELT events file from URL and filter for Saudi Arabia in one pass.
        Does NOT save the full raw file!

        Args:
            file_url: URL to the GDELT ZIP file
            target_country: Country code to filter (default: SA)

        Returns:
            Path to output parquet file if data found, None otherwise
        """
        filename = Path(file_url).name
        logger.info(f"Streaming and filtering: {filename}")

        try:
            # Extract date from filename
            date_str = filename[:8]
            file_date = datetime.strptime(date_str, "%Y%m%d")

            # Create partitioned output directory
            partition_dir = (
                self.output_dir
                / "events"
                / f"year={file_date.year}"
                / f"month={file_date.month:02d}"
                / f"day={file_date.day:02d}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f"{filename.replace('.zip', '.parquet')}"

            # Skip if already processed
            if output_file.exists():
                logger.info(f"✓ Already processed: {output_file.name}")
                return output_file

            # Download ZIP file to memory
            logger.info(f"↓ Downloading {filename}...")
            response = requests.get(file_url, stream=True, timeout=60)
            response.raise_for_status()

            # Read ZIP content into memory
            zip_content = io.BytesIO(response.content)

            # Extract CSV from ZIP (in memory)
            with zipfile.ZipFile(zip_content) as zf:
                csv_filename = zf.namelist()[0]
                csv_content = zf.read(csv_filename)

            # Save CSV temporarily for DuckDB to read
            with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".csv") as tmp:
                tmp.write(csv_content)
                tmp_path = tmp.name

            try:
                # Use DuckDB to filter and save directly to Parquet
                query = f"""
                COPY (
                    SELECT *
                    FROM read_csv('{tmp_path}',
                        delim='\t',
                        header=false,
                        columns={{
                            'GLOBALEVENTID': 'BIGINT',
                            'SQLDATE': 'INTEGER',
                            'MonthYear': 'INTEGER',
                            'Year': 'INTEGER',
                            'FractionDate': 'DOUBLE',
                            'Actor1Code': 'VARCHAR',
                            'Actor1Name': 'VARCHAR',
                            'Actor1CountryCode': 'VARCHAR',
                            'Actor1KnownGroupCode': 'VARCHAR',
                            'Actor1EthnicCode': 'VARCHAR',
                            'Actor1Religion1Code': 'VARCHAR',
                            'Actor1Religion2Code': 'VARCHAR',
                            'Actor1Type1Code': 'VARCHAR',
                            'Actor1Type2Code': 'VARCHAR',
                            'Actor1Type3Code': 'VARCHAR',
                            'Actor2Code': 'VARCHAR',
                            'Actor2Name': 'VARCHAR',
                            'Actor2CountryCode': 'VARCHAR',
                            'Actor2KnownGroupCode': 'VARCHAR',
                            'Actor2EthnicCode': 'VARCHAR',
                            'Actor2Religion1Code': 'VARCHAR',
                            'Actor2Religion2Code': 'VARCHAR',
                            'Actor2Type1Code': 'VARCHAR',
                            'Actor2Type2Code': 'VARCHAR',
                            'Actor2Type3Code': 'VARCHAR',
                            'IsRootEvent': 'INTEGER',
                            'EventCode': 'VARCHAR',
                            'EventBaseCode': 'VARCHAR',
                            'EventRootCode': 'VARCHAR',
                            'QuadClass': 'INTEGER',
                            'GoldsteinScale': 'DOUBLE',
                            'NumMentions': 'INTEGER',
                            'NumSources': 'INTEGER',
                            'NumArticles': 'INTEGER',
                            'AvgTone': 'DOUBLE',
                            'Actor1Geo_Type': 'INTEGER',
                            'Actor1Geo_FullName': 'VARCHAR',
                            'Actor1Geo_CountryCode': 'VARCHAR',
                            'Actor1Geo_ADM1Code': 'VARCHAR',
                            'Actor1Geo_ADM2Code': 'VARCHAR',
                            'Actor1Geo_Lat': 'DOUBLE',
                            'Actor1Geo_Long': 'DOUBLE',
                            'Actor1Geo_FeatureID': 'VARCHAR',
                            'Actor2Geo_Type': 'INTEGER',
                            'Actor2Geo_FullName': 'VARCHAR',
                            'Actor2Geo_CountryCode': 'VARCHAR',
                            'Actor2Geo_ADM1Code': 'VARCHAR',
                            'Actor2Geo_ADM2Code': 'VARCHAR',
                            'Actor2Geo_Lat': 'DOUBLE',
                            'Actor2Geo_Long': 'DOUBLE',
                            'Actor2Geo_FeatureID': 'VARCHAR',
                            'ActionGeo_Type': 'INTEGER',
                            'ActionGeo_FullName': 'VARCHAR',
                            'ActionGeo_CountryCode': 'VARCHAR',
                            'ActionGeo_ADM1Code': 'VARCHAR',
                            'ActionGeo_ADM2Code': 'VARCHAR',
                            'ActionGeo_Lat': 'DOUBLE',
                            'ActionGeo_Long': 'DOUBLE',
                            'ActionGeo_FeatureID': 'VARCHAR',
                            'DATEADDED': 'BIGINT',
                            'SOURCEURL': 'VARCHAR'
                        }},
                        ignore_errors=true
                    )
                    WHERE Actor1CountryCode = '{target_country}'
                       OR Actor2CountryCode = '{target_country}'
                       OR Actor1Geo_CountryCode = '{target_country}'
                       OR Actor2Geo_CountryCode = '{target_country}'
                       OR ActionGeo_CountryCode = '{target_country}'
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """

                self.conn.execute(query)

                # Check record count
                result = self.conn.execute(f"""
                    SELECT COUNT(*) as count
                    FROM read_parquet('{output_file}')
                """).fetchone()

                record_count = result[0] if result else 0

                if record_count > 0:
                    logger.info(f"✓ Found {record_count} Saudi Arabia events → {output_file.name}")
                    return output_file
                else:
                    logger.info("○ No Saudi Arabia events in this file")
                    if output_file.exists():
                        output_file.unlink()
                    return None

            finally:
                # Clean up temporary file
                Path(tmp_path).unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"✗ Failed to process {filename}: {e}")
            return None

    def stream_and_filter_mentions(self, file_url: str) -> Path | None:
        """
        Stream GDELT mentions file and save to parquet.
        For mentions, we save all (can be filtered by event IDs later if needed)

        Args:
            file_url: URL to the GDELT ZIP file

        Returns:
            Path to output parquet file if data found, None otherwise
        """
        filename = Path(file_url).name
        logger.info(f"Streaming mentions: {filename}")

        try:
            date_str = filename[:8]
            file_date = datetime.strptime(date_str, "%Y%m%d")

            partition_dir = (
                self.output_dir
                / "mentions"
                / f"year={file_date.year}"
                / f"month={file_date.month:02d}"
                / f"day={file_date.day:02d}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f"{filename.replace('.zip', '.parquet')}"

            if output_file.exists():
                logger.info(f"✓ Already processed: {output_file.name}")
                return output_file

            # Download and extract
            response = requests.get(file_url, stream=True, timeout=60)
            response.raise_for_status()
            zip_content = io.BytesIO(response.content)

            with zipfile.ZipFile(zip_content) as zf:
                csv_content = zf.read(zf.namelist()[0])

            with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".csv") as tmp:
                tmp.write(csv_content)
                tmp_path = tmp.name

            try:
                # For mentions, we'll save all (smaller files)
                # Can be joined with events later for SA-specific mentions
                query = f"""
                COPY (
                    SELECT *
                    FROM read_csv('{tmp_path}',
                        delim='\t',
                        header=false,
                        columns={{
                            'GLOBALEVENTID': 'BIGINT',
                            'EventTimeDate': 'BIGINT',
                            'MentionTimeDate': 'BIGINT',
                            'MentionType': 'INTEGER',
                            'MentionSourceName': 'VARCHAR',
                            'MentionIdentifier': 'VARCHAR',
                            'SentenceID': 'INTEGER',
                            'Actor1CharOffset': 'INTEGER',
                            'Actor2CharOffset': 'INTEGER',
                            'ActionCharOffset': 'INTEGER',
                            'InRawText': 'INTEGER',
                            'Confidence': 'INTEGER',
                            'MentionDocLen': 'INTEGER',
                            'MentionDocTone': 'DOUBLE',
                            'MentionDocTranslationInfo': 'VARCHAR',
                            'Extras': 'VARCHAR'
                        }},
                        ignore_errors=true
                    )
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """

                self.conn.execute(query)
                logger.info(f"✓ Processed mentions → {output_file.name}")
                return output_file

            finally:
                Path(tmp_path).unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"✗ Failed to process {filename}: {e}")
            return None

    def stream_and_filter_gkg(self, file_url: str, target_country: str = "SA") -> Path | None:
        """
        Stream GDELT GKG file and filter for Saudi Arabia.

        Args:
            file_url: URL to the GDELT ZIP file
            target_country: Country code to filter

        Returns:
            Path to output parquet file if data found, None otherwise
        """
        filename = Path(file_url).name
        logger.info(f"Streaming GKG: {filename}")

        try:
            date_str = filename[:8]
            file_date = datetime.strptime(date_str, "%Y%m%d")

            partition_dir = (
                self.output_dir
                / "gkg"
                / f"year={file_date.year}"
                / f"month={file_date.month:02d}"
                / f"day={file_date.day:02d}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f"{filename.replace('.zip', '.parquet')}"

            if output_file.exists():
                logger.info(f"✓ Already processed: {output_file.name}")
                return output_file

            response = requests.get(file_url, stream=True, timeout=60)
            response.raise_for_status()
            zip_content = io.BytesIO(response.content)

            with zipfile.ZipFile(zip_content) as zf:
                csv_content = zf.read(zf.namelist()[0])

            with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".csv") as tmp:
                tmp.write(csv_content)
                tmp_path = tmp.name

            try:
                # GKG filtering - look for SA in locations fields
                # Using simple string matching on the Locations field
                query = f"""
                COPY (
                    SELECT *
                    FROM read_csv('{tmp_path}',
                        delim='\t',
                        header=false,
                        ignore_errors=true,
                        all_varchar=true
                    )
                    WHERE column9 LIKE '%{target_country}#%'  -- V2Locations field typically contains country codes
                       OR column10 LIKE '%{target_country}#%'  -- Alternate location field
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """

                self.conn.execute(query)

                result = self.conn.execute(f"""
                    SELECT COUNT(*) as count
                    FROM read_parquet('{output_file}')
                """).fetchone()

                record_count = result[0] if result else 0

                if record_count > 0:
                    logger.info(f"✓ Found {record_count} GKG records → {output_file.name}")
                    return output_file
                else:
                    logger.info(f"○ No GKG records for {target_country}")
                    if output_file.exists():
                        output_file.unlink()
                    return None

            finally:
                Path(tmp_path).unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"✗ Failed to process {filename}: {e}")
            return None

    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()
