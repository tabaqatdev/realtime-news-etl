"""
Data Cleaner - Quality checks and cleaning for GDELT data
Uses DuckDB's SUMMARIZE command for smart data profiling
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def clean_events_data(
    input_pattern: str = "data/parquet/events/**/*.parquet",
    output_dir: str = "data/parquet/cleaned",
    remove_duplicates: bool = True,
    remove_nulls: bool = True,
    min_tone: float = -10.0,
    max_tone: float = 10.0,
) -> dict:
    """
    Clean and validate GDELT events data

    Args:
        input_pattern: Glob pattern for input parquet files
        output_dir: Output directory for cleaned data
        remove_duplicates: Remove duplicate events
        remove_nulls: Remove rows with null critical fields
        min_tone: Minimum valid tone score
        max_tone: Maximum valid tone score

    Returns:
        Dictionary with cleaning statistics

    Example:
        >>> stats = clean_events_data(limit=1000)
        >>> print(f"Cleaned {stats['records_after']} records")
    """
    conn = duckdb.connect(":memory:")

    try:
        # Count before
        count_before = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{input_pattern}', union_by_name=true)"
        ).fetchone()[0]

        logger.info(f"📊 Records before cleaning: {count_before:,}")

        # Build cleaning query
        where_clauses = []

        if remove_nulls:
            where_clauses.extend(
                [
                    "GLOBALEVENTID IS NOT NULL",
                    "SQLDATE IS NOT NULL",
                    "Actor1Name IS NOT NULL OR Actor2Name IS NOT NULL",
                ]
            )

        if min_tone is not None and max_tone is not None:
            where_clauses.append(f"AvgTone BETWEEN {min_tone} AND {max_tone}")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Create cleaned version
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        query = f"""
            COPY (
                SELECT {"DISTINCT" if remove_duplicates else ""} *
                FROM read_parquet('{input_pattern}', union_by_name=true)
                WHERE {where_clause}
                ORDER BY SQLDATE DESC, DATEADDED DESC
            ) TO '{output_path}/cleaned_events.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        conn.execute(query)

        # Count after
        count_after = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output_path}/cleaned_events.parquet')"
        ).fetchone()[0]

        stats = {
            "records_before": count_before,
            "records_after": count_after,
            "records_removed": count_before - count_after,
            "removal_rate": (count_before - count_after) / count_before * 100
            if count_before > 0
            else 0,
            "output_file": str(output_path / "cleaned_events.parquet"),
        }

        logger.info(f"✅ Records after cleaning: {count_after:,}")
        logger.info(f"🗑️  Removed: {stats['records_removed']:,} ({stats['removal_rate']:.1f}%)")
        logger.info(f"📁 Output: {stats['output_file']}")

        return stats

    except Exception as e:
        logger.error(f"❌ Cleaning failed: {e}")
        raise
    finally:
        conn.close()


def validate_data_quality(
    parquet_pattern: str = "data/parquet/events/**/*.parquet",
) -> dict:
    """
    Run data quality checks on GDELT events

    Args:
        parquet_pattern: Glob pattern for parquet files

    Returns:
        Dictionary with quality metrics

    Example:
        >>> metrics = validate_data_quality()
        >>> print(f"Data quality score: {metrics['quality_score']}")
    """
    conn = duckdb.connect(":memory:")

    try:
        # Total records
        total = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_pattern}', union_by_name=true)"
        ).fetchone()[0]

        # Null counts
        null_checks = conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN GLOBALEVENTID IS NULL THEN 1 ELSE 0 END) as null_event_id,
                SUM(CASE WHEN SQLDATE IS NULL THEN 1 ELSE 0 END) as null_date,
                SUM(CASE WHEN Actor1Name IS NULL AND Actor2Name IS NULL THEN 1 ELSE 0 END) as null_actors,
                SUM(CASE WHEN AvgTone IS NULL THEN 1 ELSE 0 END) as null_tone,
                SUM(CASE WHEN SOURCEURL IS NULL OR SOURCEURL = '' THEN 1 ELSE 0 END) as null_url
            FROM read_parquet('{parquet_pattern}', union_by_name=true)
        """).fetchone()

        # Duplicates
        duplicates = conn.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT GLOBALEVENTID)
            FROM read_parquet('{parquet_pattern}', union_by_name=true)
        """).fetchone()[0]

        # Date range
        date_range = conn.execute(f"""
            SELECT MIN(SQLDATE), MAX(SQLDATE)
            FROM read_parquet('{parquet_pattern}', union_by_name=true)
        """).fetchone()

        metrics = {
            "total_records": total,
            "null_event_id": null_checks[1],
            "null_date": null_checks[2],
            "null_actors": null_checks[3],
            "null_tone": null_checks[4],
            "null_url": null_checks[5],
            "duplicate_records": duplicates,
            "date_range": {
                "min": date_range[0],
                "max": date_range[1],
            },
            "quality_score": (
                (total - null_checks[1] - null_checks[2] - duplicates) / total * 100
                if total > 0
                else 0
            ),
        }

        logger.info("📊 Data Quality Metrics:")
        logger.info(f"   Total records: {metrics['total_records']:,}")
        logger.info(f"   Null event IDs: {metrics['null_event_id']:,}")
        logger.info(f"   Null dates: {metrics['null_date']:,}")
        logger.info(f"   Null actors: {metrics['null_actors']:,}")
        logger.info(f"   Null tones: {metrics['null_tone']:,}")
        logger.info(f"   Null URLs: {metrics['null_url']:,}")
        logger.info(f"   Duplicates: {metrics['duplicate_records']:,}")
        logger.info(
            f"   Date range: {metrics['date_range']['min']} to {metrics['date_range']['max']}"
        )
        logger.info(f"   Quality score: {metrics['quality_score']:.1f}%")

        return metrics

    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        raise
    finally:
        conn.close()


def smart_profile_data(
    parquet_pattern: str = "data/parquet/events/**/*.parquet",
    output_file: str | None = None,
) -> dict:
    """
    Use DuckDB's SUMMARIZE command for comprehensive data profiling

    Computes statistics for all columns: min, max, approx_unique, avg, std,
    q25, q50, q75, count, and null_percentage

    Args:
        parquet_pattern: Glob pattern for parquet files
        output_file: Optional path to save summary as CSV

    Returns:
        Dictionary with complete data profile

    Example:
        >>> profile = smart_profile_data()
        >>> print(f"Columns profiled: {len(profile['summary'])}")
    """
    conn = duckdb.connect(":memory:")

    try:
        logger.info("🔍 Running DuckDB SUMMARIZE on data...")

        # Use DuckDB's SUMMARIZE command
        summary_result = conn.execute(f"""
            SUMMARIZE SELECT * FROM read_parquet('{parquet_pattern}', union_by_name=true)
        """).fetchall()

        # Get column names
        summary_columns = [desc[0] for desc in conn.description]

        # Convert to list of dicts
        summary_records = [dict(zip(summary_columns, row, strict=False)) for row in summary_result]

        profile = {
            "summary": summary_records,
            "total_columns": len(summary_records),
            "total_records": summary_records[0]["count"] if len(summary_records) > 0 else 0,
        }

        # Save to file if requested
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write to CSV manually
            with open(output_path, "w") as f:
                # Write header
                if summary_records:
                    f.write(",".join(summary_columns) + "\n")
                    # Write rows
                    for record in summary_records:
                        values = [str(record.get(col, "")) for col in summary_columns]
                        f.write(",".join(f'"{v}"' for v in values) + "\n")

            logger.info(f"📁 Saved profile to: {output_path}")

        # Print summary
        logger.info("📊 Data Profile Summary:")
        logger.info(f"   Total columns: {profile['total_columns']}")
        logger.info(f"   Total records: {profile['total_records']:,}")

        # Show columns with high null percentages
        high_null_cols = []
        for row in profile["summary"]:
            null_pct = row.get("null_percentage", "0%")
            if null_pct and isinstance(null_pct, str):
                pct_value = float(null_pct.rstrip("%"))
                if pct_value > 10:
                    high_null_cols.append(row)

        if high_null_cols:
            logger.info(f"   ⚠️  Columns with >10% nulls: {len(high_null_cols)}")
            for col in high_null_cols[:5]:  # Show first 5
                logger.info(f"      - {col['column_name']}: {col['null_percentage']}")

        return profile

    except Exception as e:
        logger.error(f"❌ Profiling failed: {e}")
        raise
    finally:
        conn.close()


def unify_data(
    input_patterns: list[str],
    output_file: str = "data/parquet/unified/unified_events.parquet",
    deduplicate: bool = True,
) -> dict:
    """
    Unify multiple parquet datasets into a single consolidated file

    Args:
        input_patterns: List of glob patterns for input parquet files
        output_file: Output path for unified parquet file
        deduplicate: Remove duplicate records based on GLOBALEVENTID

    Returns:
        Dictionary with unification statistics

    Example:
        >>> stats = unify_data(['data/parquet/events/**/*.parquet'])
        >>> print(f"Unified {stats['total_records']:,} records")
    """
    conn = duckdb.connect(":memory:")

    try:
        logger.info(f"🔄 Unifying {len(input_patterns)} data sources...")

        # Build UNION query
        union_parts = [f"SELECT * FROM read_parquet('{pattern}')" for pattern in input_patterns]
        union_query = " UNION ALL ".join(union_parts)

        # Count before deduplication
        count_before = conn.execute(f"""
            SELECT COUNT(*) FROM ({union_query})
        """).fetchone()[0]

        logger.info(f"📊 Records before unification: {count_before:,}")

        # Create unified file
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        dedup_clause = "DISTINCT" if deduplicate else ""

        conn.execute(f"""
            COPY (
                SELECT {dedup_clause} *
                FROM ({union_query})
                ORDER BY SQLDATE DESC, DATEADDED DESC
            ) TO '{output_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Count after
        count_after = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]

        stats = {
            "sources": len(input_patterns),
            "records_before": count_before,
            "records_after": count_after,
            "duplicates_removed": count_before - count_after if deduplicate else 0,
            "output_file": str(output_path),
        }

        logger.info(f"✅ Unified {stats['records_after']:,} records")
        if deduplicate and stats["duplicates_removed"] > 0:
            logger.info(f"🗑️  Removed {stats['duplicates_removed']:,} duplicates")
        logger.info(f"📁 Output: {stats['output_file']}")

        return stats

    except Exception as e:
        logger.error(f"❌ Unification failed: {e}")
        raise
    finally:
        conn.close()


def main():
    """CLI entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Clean and validate GDELT data")
    parser.add_argument(
        "--action",
        choices=["clean", "validate", "profile", "unify"],
        default="validate",
        help="Action to perform (default: validate)",
    )
    parser.add_argument(
        "--input",
        default="data/parquet/events/**/*.parquet",
        help="Input parquet pattern (default: data/parquet/events/**/*.parquet)",
    )
    parser.add_argument(
        "--output",
        default="data/parquet/cleaned",
        help="Output directory for cleaned data (default: data/parquet/cleaned)",
    )
    parser.add_argument(
        "--no-remove-duplicates",
        action="store_true",
        help="Keep duplicate records",
    )
    parser.add_argument(
        "--no-remove-nulls",
        action="store_true",
        help="Keep records with null values",
    )
    parser.add_argument(
        "--profile-output",
        help="Output file for data profile (CSV)",
    )
    parser.add_argument(
        "--unify-sources",
        nargs="+",
        help="Multiple glob patterns for unification",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    print("=" * 70)
    print(" GDELT DATA CLEANER")
    print("=" * 70)

    if args.action == "validate":
        print("🔍 Running data quality validation...")
        metrics = validate_data_quality(args.input)
        print(f"\n✅ Validation complete - Quality score: {metrics['quality_score']:.1f}%")

    elif args.action == "clean":
        print("🧹 Cleaning data...")
        stats = clean_events_data(
            input_pattern=args.input,
            output_dir=args.output,
            remove_duplicates=not args.no_remove_duplicates,
            remove_nulls=not args.no_remove_nulls,
        )
        print(f"\n✅ Cleaning complete - Removed {stats['records_removed']:,} records")

    elif args.action == "profile":
        print("📊 Running smart data profiling (DuckDB SUMMARIZE)...")
        profile = smart_profile_data(args.input, args.profile_output)
        print(f"\n✅ Profile complete - {profile['total_columns']} columns analyzed")

    elif args.action == "unify":
        if not args.unify_sources:
            print("❌ Error: --unify-sources required for unify action")
            return
        print("🔄 Unifying data sources...")
        stats = unify_data(args.unify_sources, args.output)
        print(f"\n✅ Unified {stats['records_after']:,} records from {stats['sources']} sources")


if __name__ == "__main__":
    main()
