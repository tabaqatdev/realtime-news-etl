"""
Partition Utilities: Hive-style Partitioning for GDELT Data
Provides consistent directory management for time-series data
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

DataType = Literal["export", "mentions", "gkg"]


def ensure_partition_dir(
    base_dir: Path | str,
    date: datetime | str,
    data_type: DataType = "export",
) -> Path:
    """
    Create Hive-style partition directory structure

    Args:
        base_dir: Base output directory
        date: Date object or date string (YYYYMMDD format)
        data_type: Type of GDELT data ("export", "mentions", "gkg")

    Returns:
        Path to the partition directory

    Example:
        >>> ensure_partition_dir("data/parquet", datetime(2026, 1, 15), "export")
        Path("data/parquet/events/year=2026/month=01/day=15")
    """
    base_path = Path(base_dir)

    # Parse date if string
    if isinstance(date, str):
        if len(date) == 8:  # YYYYMMDD
            date = datetime.strptime(date, "%Y%m%d")
        else:
            raise ValueError(f"Date string must be in YYYYMMDD format, got: {date}")

    # Map data type to subdirectory
    type_dir_map = {
        "export": "events",
        "mentions": "mentions",
        "gkg": "gkg",
    }

    if data_type not in type_dir_map:
        raise ValueError(
            f"Unknown data type: {data_type}. Must be one of {list(type_dir_map.keys())}"
        )

    # Build partition path
    partition_dir = (
        base_path
        / type_dir_map[data_type]
        / f"year={date.year}"
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
    )

    # Create directory
    partition_dir.mkdir(parents=True, exist_ok=True)

    logger.debug(f"Ensured partition directory: {partition_dir}")
    return partition_dir


def get_partition_path(
    base_dir: Path | str,
    date: datetime | str,
    filename: str,
    data_type: DataType = "export",
) -> Path:
    """
    Get full path for a partitioned file

    Args:
        base_dir: Base output directory
        date: Date object or date string (YYYYMMDD format)
        filename: Name of the file
        data_type: Type of GDELT data

    Returns:
        Full path to the file in partition directory

    Example:
        >>> get_partition_path("data/parquet", "20260115", "consolidated.parquet")
        Path("data/parquet/events/year=2026/month=01/day=15/consolidated.parquet")
    """
    partition_dir = ensure_partition_dir(base_dir, date, data_type)
    return partition_dir / filename


def get_consolidated_filename(date_str: str, data_type: DataType = "export") -> str:
    """
    Get standard consolidated filename for a date

    Args:
        date_str: Date string in YYYYMMDD format
        data_type: Type of GDELT data

    Returns:
        Filename string

    Example:
        >>> get_consolidated_filename("20260115")
        "20260115_consolidated.parquet"
    """
    return f"{date_str}_consolidated.parquet"


def parse_date_from_filename(filename: str) -> str:
    """
    Extract date (YYYYMMDD) from GDELT filename

    Args:
        filename: GDELT filename (e.g., "20260115123000.export.CSV.zip")

    Returns:
        Date string in YYYYMMDD format

    Example:
        >>> parse_date_from_filename("20260115123000.export.CSV.zip")
        "20260115"
    """
    return filename[:8]


def group_files_by_date(file_list: list[tuple[str, str]]) -> dict[str, list[str]]:
    """
    Group GDELT file URLs by date

    Args:
        file_list: List of tuples (file_size, file_url)

    Returns:
        Dictionary mapping date_str -> list of URLs

    Example:
        >>> files = [("92K", "http://.../20260115000000.export.CSV.zip"),
        ...          ("61K", "http://.../20260115001500.export.CSV.zip")]
        >>> group_files_by_date(files)
        {"20260115": ["http://.../20260115000000.export.CSV.zip", ...]}
    """
    grouped: dict[str, list[str]] = {}

    for _file_size, file_url in file_list:
        filename = file_url.split("/")[-1]
        date_str = parse_date_from_filename(filename)

        if date_str not in grouped:
            grouped[date_str] = []
        grouped[date_str].append(file_url)

    logger.debug(f"Grouped {len(file_list)} files into {len(grouped)} days")
    return grouped


def get_glob_pattern(base_dir: Path | str, data_type: DataType = "export") -> str:
    """
    Get glob pattern for reading all parquet files

    Args:
        base_dir: Base output directory
        data_type: Type of GDELT data

    Returns:
        Glob pattern string for DuckDB

    Example:
        >>> get_glob_pattern("data/parquet", "export")
        "data/parquet/events/**/*.parquet"
    """
    base_path = Path(base_dir)
    type_dir_map = {
        "export": "events",
        "mentions": "mentions",
        "gkg": "gkg",
    }

    return str(base_path / type_dir_map[data_type] / "**" / "*.parquet")
