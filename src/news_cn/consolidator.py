"""
Data Consolidation Module
Consolidates multiple 15-minute parquet files into daily files
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class DataConsolidator:
    """Consolidates parquet files into daily aggregates"""

    def __init__(self, parquet_dir: Path | None = None):
        self.parquet_dir = parquet_dir or Path("data/parquet")

    def consolidate_day(
        self, date_str: str, data_type: str = "events", delete_originals: bool = False
    ) -> dict | None:
        """
        Consolidate all 15-minute files for a specific day into a single daily file

        Args:
            date_str: Date in YYYYMMDD format
            data_type: Type of data (events, mentions, gkg)
            delete_originals: Whether to delete original 15-minute files

        Returns:
            Dictionary with consolidation stats or None if no files found
        """
        # Parse date for directory structure
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]

        # Find all parquet files for this day
        day_dir = self.parquet_dir / data_type / f"year={year}" / f"month={month}" / f"day={day}"

        if not day_dir.exists():
            logger.info(f"No data directory found for {date_str}")
            return None

        parquet_files = list(day_dir.glob("*.parquet"))

        if not parquet_files:
            logger.info(f"No parquet files found for {date_str}")
            return None

        logger.info(f"Consolidating {len(parquet_files)} files for {date_str}...")
        files_count = len(parquet_files)

        try:
            # Create consolidated filename
            consolidated_file = day_dir / f"{date_str}_consolidated.parquet"

            # Use DuckDB to read all files and write consolidated version
            con = duckdb.connect(":memory:")

            # Read all parquet files for this day
            pattern = str(day_dir / "*.parquet")
            query = f"""
                COPY (
                    SELECT DISTINCT *
                    FROM read_parquet('{pattern}')
                    ORDER BY GLOBALEVENTID
                ) TO '{consolidated_file}'
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """

            con.execute(query)
            con.close()

            logger.info(f"✓ Consolidated to: {consolidated_file.name}")

            # Optionally delete original files
            if delete_originals:
                for pf in parquet_files:
                    if pf != consolidated_file:  # Don't delete the consolidated file
                        pf.unlink()
                        logger.debug(f"Deleted: {pf.name}")
                logger.info(f"Deleted {files_count} original files (kept consolidated file)")

            return {
                "date": date_str,
                "files_merged": files_count,
                "output_file": consolidated_file,
                "deleted_originals": delete_originals,
            }

        except Exception as e:
            logger.error(f"Failed to consolidate {date_str}: {e}")
            return None

    def consolidate_all_pending_days(self, data_type: str = "events") -> list[str]:
        """
        Find all days that have multiple parquet files and consolidate them

        Args:
            data_type: Type of data (events, mentions, gkg)

        Returns:
            List of consolidated date strings
        """
        consolidated_dates = []

        # Find all year directories
        type_dir = self.parquet_dir / data_type
        if not type_dir.exists():
            logger.warning(f"No {data_type} directory found")
            return consolidated_dates

        # Traverse year/month/day structure
        for year_dir in sorted(type_dir.glob("year=*")):
            for month_dir in sorted(year_dir.glob("month=*")):
                for day_dir in sorted(month_dir.glob("day=*")):
                    # Check if this day has multiple files and no consolidated file
                    parquet_files = list(day_dir.glob("*.parquet"))
                    consolidated_exists = any("_consolidated" in f.name for f in parquet_files)

                    if len(parquet_files) > 1 and not consolidated_exists:
                        # Extract date from directory structure
                        year = year_dir.name.split("=")[1]
                        month = month_dir.name.split("=")[1]
                        day_name = day_dir.name.split("=")[1]
                        date_str = f"{year}{month}{day_name}"

                        result = self.consolidate_day(date_str, data_type, delete_originals=False)
                        if result:
                            consolidated_dates.append(date_str)

        return consolidated_dates

    def get_stats(self, data_type: str = "events") -> dict:
        """
        Get consolidation statistics

        Args:
            data_type: Type of data (events, mentions, gkg)

        Returns:
            Dictionary with stats
        """
        type_dir = self.parquet_dir / data_type
        if not type_dir.exists():
            return {"total_days": 0, "consolidated_days": 0, "unconsolidated_days": 0}

        total_days = 0
        consolidated_days = 0
        unconsolidated_days = 0

        for year_dir in type_dir.glob("year=*"):
            for month_dir in year_dir.glob("month=*"):
                for day_dir in month_dir.glob("day=*"):
                    parquet_files = list(day_dir.glob("*.parquet"))
                    if parquet_files:
                        total_days += 1
                        has_consolidated = any("_consolidated" in f.name for f in parquet_files)
                        if has_consolidated:
                            consolidated_days += 1
                        elif len(parquet_files) > 1:
                            unconsolidated_days += 1

        return {
            "total_days": total_days,
            "consolidated_days": consolidated_days,
            "unconsolidated_days": unconsolidated_days,
        }
