"""
DuckDB Utilities: Connection Management and Query Building
Provides reusable patterns for DuckDB operations
"""

import logging
from contextlib import contextmanager
from dataclasses import dataclass

import duckdb

from .config import COUNTRY_CODE_MAP

logger = logging.getLogger(__name__)


@dataclass
class DuckDBConfig:
    """Configuration for DuckDB connections"""

    memory_limit: str = "4GB"
    threads: int = 4
    enable_progress_bar: bool = True
    compression: str = "ZSTD"
    temp_directory: str | None = None


class DuckDBConnectionManager:
    """
    Manages DuckDB connections with automatic resource cleanup
    Supports context manager protocol for safe connection handling
    """

    def __init__(self, config: DuckDBConfig | None = None, database: str = ":memory:"):
        self.config = config or DuckDBConfig()
        self.database = database
        self.conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self):
        """Context manager entry - create connection"""
        self.conn = self.connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection"""
        self.close()
        return False

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Create and configure DuckDB connection

        Returns:
            Configured DuckDB connection
        """
        conn = duckdb.connect(self.database)

        # Apply configuration
        conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
        conn.execute(f"SET threads={self.config.threads}")

        if self.config.enable_progress_bar:
            conn.execute("SET enable_progress_bar=true")

        if self.config.temp_directory:
            conn.execute(f"SET temp_directory='{self.config.temp_directory}'")

        logger.debug(
            f"DuckDB connection initialized: memory={self.config.memory_limit}, "
            f"threads={self.config.threads}"
        )

        return conn

    def close(self):
        """Close the connection if open"""
        if self.conn:
            try:
                self.conn.close()
                logger.debug("DuckDB connection closed")
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {e}")
            finally:
                self.conn = None

    @contextmanager
    def get_connection(self):
        """
        Get a connection as a context manager

        Usage:
            with manager.get_connection() as conn:
                conn.execute(query)
        """
        if self.conn is None:
            self.conn = self.connect()

        try:
            yield self.conn
        finally:
            # Keep connection open for reuse unless explicitly closed
            pass


class DuckDBQueryBuilder:
    """
    Builder pattern for DuckDB queries
    Simplifies construction of common GDELT processing queries
    """

    def __init__(self, config: DuckDBConfig | None = None):
        self.config = config or DuckDBConfig()
        self.select_columns: list[str] = []
        self.from_clause: str = ""
        self.where_conditions: list[str] = []
        self.output_file: str | None = None
        self.normalize_countries: bool = False

    def select(self, columns: list[str]) -> "DuckDBQueryBuilder":
        """Set SELECT columns"""
        self.select_columns = columns
        return self

    def from_csv(
        self,
        csv_path: str,
        columns: dict[str, str],
        delimiter: str = "\t",
        header: bool = False,
    ) -> "DuckDBQueryBuilder":
        """
        Set FROM clause for CSV reading

        Args:
            csv_path: Path or glob pattern for CSV files
            columns: Column name to type mapping
            delimiter: CSV delimiter
            header: Whether CSV has header row
        """
        col_defs = ", ".join([f"'{k}': '{v}'" for k, v in columns.items()])
        self.from_clause = f"""read_csv(
            '{csv_path}',
            delim='{delimiter}',
            header={"true" if header else "false"},
            columns={{{col_defs}}},
            ignore_errors=true
        )"""
        return self

    def where(self, condition: str) -> "DuckDBQueryBuilder":
        """Add WHERE condition"""
        self.where_conditions.append(condition)
        return self

    def where_country(self, country_code: str, geo_only: bool = False) -> "DuckDBQueryBuilder":
        """
        Add country filter conditions

        Args:
            country_code: 2-letter country code (e.g., "SA")
            geo_only: If True, only filter by geography (not actor codes)
        """
        conditions = [
            f"Actor1Geo_CountryCode = '{country_code}'",
            f"Actor2Geo_CountryCode = '{country_code}'",
            f"ActionGeo_CountryCode = '{country_code}'",
        ]

        if not geo_only:
            conditions.extend(
                [
                    f"Actor1CountryCode = '{country_code}'",
                    f"Actor2CountryCode = '{country_code}'",
                ]
            )

        # Combine with OR
        combined = " OR ".join(conditions)
        self.where_conditions.append(f"({combined})")
        return self

    def with_country_normalization(self) -> "DuckDBQueryBuilder":
        """Enable country code normalization (3-letter to 2-letter)"""
        self.normalize_countries = True
        return self

    def to_parquet(self, output_path: str) -> "DuckDBQueryBuilder":
        """Set output to Parquet file"""
        self.output_file = output_path
        return self

    def build(self) -> str:
        """
        Build the final SQL query

        Returns:
            Complete DuckDB SQL query string
        """
        # Build SELECT clause with optional country normalization
        select_parts = []
        for col in self.select_columns:
            if self.normalize_countries and col in ["Actor1CountryCode", "Actor2CountryCode"]:
                # Build CASE statement for normalization
                case_expr = f"CASE {col} "
                for old_code, new_code in COUNTRY_CODE_MAP.items():
                    case_expr += f"WHEN '{old_code}' THEN '{new_code}' "
                case_expr += f"ELSE {col} END AS {col}"
                select_parts.append(case_expr)
            else:
                select_parts.append(col)

        select_clause = ",\n                    ".join(select_parts)

        # Build WHERE clause
        where_clause = ""
        if self.where_conditions:
            where_clause = "WHERE " + " AND ".join(self.where_conditions)

        # Build complete query
        if self.output_file:
            query = f"""
            COPY (
                SELECT
                    {select_clause}
                FROM {self.from_clause}
                {where_clause}
            ) TO '{self.output_file}' (FORMAT PARQUET, COMPRESSION {self.config.compression})
            """
        else:
            query = f"""
            SELECT
                {select_clause}
            FROM {self.from_clause}
            {where_clause}
            """

        return query.strip()

    def execute(self, conn: duckdb.DuckDBPyConnection) -> list | None:
        """
        Execute the built query

        Args:
            conn: DuckDB connection

        Returns:
            Query results (None if query writes to file)
        """
        query = self.build()
        logger.debug(f"Executing query: {query[:100]}...")

        try:
            result = conn.execute(query)
            if self.output_file:
                return None
            return result.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise


def quick_query(
    csv_path: str,
    columns: dict[str, str],
    select_columns: list[str],
    output_path: str,
    country_code: str = "SA",
    config: DuckDBConfig | None = None,
) -> None:
    """
    Convenience function for common GDELT query pattern

    Args:
        csv_path: Path or glob pattern for CSV files
        columns: Full column definitions for parsing
        select_columns: Columns to select (essential columns)
        output_path: Output Parquet file path
        country_code: Country to filter by
        config: DuckDB configuration (uses defaults if None)
    """
    config = config or DuckDBConfig()

    with DuckDBConnectionManager(config) as conn:
        query = (
            DuckDBQueryBuilder(config)
            .select(select_columns)
            .from_csv(csv_path, columns)
            .where_country(country_code)
            .with_country_normalization()
            .to_parquet(output_path)
            .build()
        )

        conn.execute(query)
        logger.info(f"Query completed: {output_path}")
