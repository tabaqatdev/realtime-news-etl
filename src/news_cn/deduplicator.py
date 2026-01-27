"""
Smart Deduplication - Choose best record per unique URL
Selects records with most complete data using data quality scoring
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class SmartDeduplicator:
    """
    Intelligently deduplicate events by URL, keeping the record with:
    1. Most complete geographic data (all 3 coordinate sets preferred)
    2. Most complete actor information
    3. Most metadata fields populated
    """

    def __init__(self):
        self.conn = duckdb.connect(":memory:")

    def deduplicate_by_url(
        self, input_parquet: str, output_parquet: str, keep_all_nulls: bool = False
    ) -> dict:
        """
        Deduplicate events by SOURCEURL, keeping the record with best data quality

        Args:
            input_parquet: Path to input parquet file
            output_parquet: Path to output deduplicated parquet
            keep_all_nulls: If True, keep records even if URL is NULL

        Returns:
            Dictionary with deduplication statistics
        """
        logger.info("🔍 Smart deduplication: Analyzing records by URL...")

        # Calculate data quality score for each record
        # Higher score = more complete data
        quality_score_query = """
        WITH scored_records AS (
            SELECT
                *,
                -- Geographic completeness (max 30 points)
                (CASE WHEN ActionGeo_Lat IS NOT NULL AND ActionGeo_Long IS NOT NULL THEN 10 ELSE 0 END +
                 CASE WHEN Actor1Geo_Lat IS NOT NULL AND Actor1Geo_Long IS NOT NULL THEN 10 ELSE 0 END +
                 CASE WHEN Actor2Geo_Lat IS NOT NULL AND Actor2Geo_Long IS NOT NULL THEN 10 ELSE 0 END) +

                -- Actor information completeness (max 20 points)
                (CASE WHEN Actor1Name IS NOT NULL THEN 5 ELSE 0 END +
                 CASE WHEN Actor1CountryCode IS NOT NULL THEN 5 ELSE 0 END +
                 CASE WHEN Actor2Name IS NOT NULL THEN 5 ELSE 0 END +
                 CASE WHEN Actor2CountryCode IS NOT NULL THEN 5 ELSE 0 END) +

                -- Event metadata completeness (max 20 points)
                (CASE WHEN EventCode IS NOT NULL THEN 5 ELSE 0 END +
                 CASE WHEN QuadClass IS NOT NULL THEN 5 ELSE 0 END +
                 CASE WHEN GoldsteinScale IS NOT NULL THEN 5 ELSE 0 END +
                 CASE WHEN NumMentions IS NOT NULL AND NumMentions > 0 THEN 5 ELSE 0 END) +

                -- Additional fields (max 30 points)
                (CASE WHEN AvgTone IS NOT NULL THEN 5 ELSE 0 END +
                 CASE WHEN NumArticles IS NOT NULL AND NumArticles > 0 THEN 10 ELSE 0 END +
                 CASE WHEN NumSources IS NOT NULL AND NumSources > 0 THEN 10 ELSE 0 END +
                 CASE WHEN ActionGeo_FullName IS NOT NULL THEN 5 ELSE 0 END)

                AS quality_score,

                -- Row number partitioned by URL, ordered by quality score
                ROW_NUMBER() OVER (
                    PARTITION BY SOURCEURL
                    ORDER BY
                        -- Highest quality score first
                        quality_score DESC,
                        -- Most recent event first (if tied)
                        SQLDATE DESC,
                        -- Most mentions/articles (if still tied)
                        COALESCE(NumMentions, 0) DESC,
                        COALESCE(NumArticles, 0) DESC
                ) as url_rank
            FROM read_parquet('{input_parquet}')
            WHERE SOURCEURL IS NOT NULL AND SOURCEURL != ''
        )
        SELECT * FROM scored_records
        WHERE url_rank = 1
        ORDER BY SQLDATE DESC, quality_score DESC
        """

        query = quality_score_query.format(input_parquet=input_parquet)

        # Get stats before deduplication
        stats_before = self.conn.execute(
            f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT SOURCEURL) as unique_urls,
                COUNT(*) - COUNT(DISTINCT SOURCEURL) as duplicates
            FROM read_parquet('{input_parquet}')
            WHERE SOURCEURL IS NOT NULL AND SOURCEURL != ''
        """
        ).fetchone()

        # Export deduplicated data
        Path(output_parquet).parent.mkdir(parents=True, exist_ok=True)
        self.conn.execute(
            f"COPY ({query}) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        # Get stats after deduplication
        stats_after = self.conn.execute(
            f"""
            SELECT
                COUNT(*) as total_records,
                MIN(quality_score) as min_score,
                MAX(quality_score) as max_score,
                ROUND(AVG(quality_score), 1) as avg_score
            FROM read_parquet('{output_parquet}')
        """
        ).fetchone()

        # Show quality distribution
        quality_dist = self.conn.execute(
            f"""
            SELECT
                CASE
                    WHEN quality_score >= 80 THEN 'Excellent (80-100)'
                    WHEN quality_score >= 60 THEN 'Good (60-79)'
                    WHEN quality_score >= 40 THEN 'Fair (40-59)'
                    ELSE 'Poor (<40)'
                END as quality_tier,
                COUNT(*) as records
            FROM read_parquet('{output_parquet}')
            GROUP BY quality_tier
            ORDER BY MIN(quality_score) DESC
        """
        ).fetchall()

        logger.info("📊 Deduplication Results:")
        logger.info(f"   Before: {stats_before[0]:,} records, {stats_before[1]:,} unique URLs")
        logger.info(f"   Duplicates removed: {stats_before[2]:,} records")
        logger.info(f"   After: {stats_after[0]:,} records (one per URL)")
        logger.info(
            f"   Quality scores: min={stats_after[1]}, max={stats_after[2]}, avg={stats_after[3]}"
        )
        logger.info("\n📈 Quality Distribution:")
        for tier, count in quality_dist:
            pct = 100.0 * count / stats_after[0]
            logger.info(f"   {tier}: {count:,} ({pct:.1f}%)")

        logger.info(f"📁 Output: {output_parquet}")

        return {
            "records_before": stats_before[0],
            "unique_urls": stats_before[1],
            "duplicates_removed": stats_before[2],
            "records_after": stats_after[0],
            "avg_quality_score": stats_after[3],
            "quality_distribution": dict(quality_dist),
        }

    def close(self):
        """Close DuckDB connection"""
        self.conn.close()


def deduplicate_events(
    input_parquet: str = "data/parquet/cleaned/cleaned_events.parquet",
    output_parquet: str = "data/parquet/cleaned/deduplicated_events.parquet",
) -> dict:
    """
    Convenience function for deduplication

    Args:
        input_parquet: Input parquet file path
        output_parquet: Output parquet file path

    Returns:
        Deduplication statistics
    """
    deduplicator = SmartDeduplicator()
    stats = deduplicator.deduplicate_by_url(input_parquet, output_parquet)
    deduplicator.close()
    return stats


def main():
    """CLI entry point for testing deduplication"""
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    input_file = sys.argv[1] if len(sys.argv) > 1 else "data/parquet/cleaned/cleaned_events.parquet"
    output_file = (
        sys.argv[2] if len(sys.argv) > 2 else "data/parquet/cleaned/deduplicated_events.parquet"
    )

    print("=" * 70)
    print(" SMART DEDUPLICATION - Best Record Per URL")
    print("=" * 70)
    print(f"\nInput:  {input_file}")
    print(f"Output: {output_file}\n")

    stats = deduplicate_events(input_file, output_file)

    print("\n✅ Deduplication complete!")
    print(f"   Removed {stats['duplicates_removed']:,} duplicate records")
    print(f"   Kept {stats['records_after']:,} best records (one per unique URL)")


if __name__ == "__main__":
    main()
