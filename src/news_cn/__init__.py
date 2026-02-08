"""
GDELT News Collection Pipeline

A production-ready package for downloading and processing GDELT data
with efficient filtering, parallel processing, and Parquet export.

Quick Start:
    >>> from news_cn import collect_news
    >>> results = collect_news(country="SA", start_date="2026-01-01")

    Or use the fluent API:
    >>> from news_cn import SimplePipeline
    >>> pipeline = (SimplePipeline()
    ...     .for_country("SA")
    ...     .from_date("2026-01-01")
    ...     .use_batch_processing()
    ...     .run())
"""

__version__ = "0.3.0"

from .config import Config
from .downloader import GDELTDownloader
from .duckdb_utils import DuckDBConfig, DuckDBConnectionManager, DuckDBQueryBuilder
from .partition_utils import ensure_partition_dir, group_files_by_date
from .schemas import GDELTSchema, SchemaFactory
from .simple import (
    SimplePipeline,
    collect_news,
    collect_saudi_news,
    collect_uae_news,
    query_news,
)
from .unified_processor import GDELTProcessor

__all__ = [
    # Simple API
    "collect_news",
    "collect_saudi_news",
    "collect_uae_news",
    "query_news",
    "SimplePipeline",
    # Core components
    "Config",
    "GDELTDownloader",
    "GDELTProcessor",
    # Utilities
    "SchemaFactory",
    "GDELTSchema",
    "DuckDBConnectionManager",
    "DuckDBQueryBuilder",
    "DuckDBConfig",
    "ensure_partition_dir",
    "group_files_by_date",
    # Metadata
    "__version__",
]
