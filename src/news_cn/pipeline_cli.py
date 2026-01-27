"""
Unified Pipeline CLI - Run complete GDELT pipeline with one command
Orchestrates: Download → Process → Clean → Scrape → Analyze
"""

import logging
from datetime import datetime
from pathlib import Path

from .data_cleaner import clean_events_data, validate_data_quality
from .deduplicator import SmartDeduplicator
from .geo_corrector import GeoCorrector
from .modern_scraper import ModernArticleScraper
from .simple import collect_news, query_news

logger = logging.getLogger(__name__)


class UnifiedPipeline:
    """Complete GDELT pipeline orchestrator"""

    def __init__(
        self,
        country: str = "SA",
        start_date: str = "2026-01-01",
        output_dir: str = "data",
        strategy: str = "batch",
    ):
        self.country = country
        self.start_date = start_date
        self.output_dir = Path(output_dir)
        self.strategy = strategy

        # Paths
        self.parquet_dir = self.output_dir / "parquet"
        self.articles_dir = self.output_dir / "articles"
        self.cleaned_dir = self.output_dir / "parquet" / "cleaned"

    def print_banner(self):
        """Print pipeline banner"""
        banner = f"""
╔══════════════════════════════════════════════════════════════════════╗
║             GDELT UNIFIED PIPELINE - All-in-One                      ║
║  Collect → Process → Clean → Dedupe → Geo → Scrape → Analyze        ║
╚══════════════════════════════════════════════════════════════════════╝

📍 Country: {self.country}
📅 Start Date: {self.start_date}
📁 Output: {self.output_dir}
⚡ Strategy: {self.strategy}
"""
        print(banner)

    def step_1_collect(self) -> dict:
        """Step 1: Collect GDELT data"""
        print("\n" + "=" * 70)
        print("STEP 1/6: COLLECTING GDELT DATA")
        print("=" * 70)

        results = collect_news(
            country=self.country,
            start_date=self.start_date,
            output_dir=str(self.parquet_dir),
            strategy=self.strategy,
        )

        print(f"✅ Collected {len(results)} days of data")
        return results

    def step_2_validate(self) -> dict:
        """Step 2: Validate data quality"""
        print("\n" + "=" * 70)
        print("STEP 2/6: VALIDATING DATA QUALITY")
        print("=" * 70)

        parquet_pattern = str(self.parquet_dir / "events/**/*.parquet")
        metrics = validate_data_quality(parquet_pattern)

        print(f"✅ Quality score: {metrics['quality_score']:.1f}%")
        return metrics

    def step_3_clean(self, enrich_geo: bool = False, deduplicate: bool = True) -> dict:
        """Step 3: Clean data and optionally deduplicate by URL"""
        print("\n" + "=" * 70)
        print("STEP 3/6: CLEANING DATA")
        print("=" * 70)

        parquet_pattern = str(self.parquet_dir / "events/**/*.parquet")
        stats = clean_events_data(
            input_pattern=parquet_pattern,
            output_dir=str(self.cleaned_dir),
            remove_duplicates=True,
            remove_nulls=True,
        )

        print(f"✅ Cleaned {stats['records_after']:,} records")

        # Smart deduplication by URL (keep best record per URL)
        if deduplicate:
            print("\n🔍 Smart deduplication: Selecting best record per unique URL...")
            deduplicator = SmartDeduplicator()
            try:
                dedup_input = str(self.cleaned_dir / "cleaned_events.parquet")
                dedup_output = str(self.cleaned_dir / "deduplicated_events.parquet")

                dedup_stats = deduplicator.deduplicate_by_url(dedup_input, dedup_output)

                print(f"✅ Removed {dedup_stats['duplicates_removed']:,} duplicates")
                print(f"   Kept {dedup_stats['records_after']:,} best records (one per URL)")
                print(f"   Avg quality score: {dedup_stats['avg_quality_score']}")

                stats["duplicates_removed"] = dedup_stats["duplicates_removed"]
                stats["after_deduplication"] = dedup_stats["records_after"]
                stats["avg_quality_score"] = dedup_stats["avg_quality_score"]

                # Use deduplicated file for downstream processing
                working_file = dedup_output
            except Exception as e:
                logger.warning(f"⚠️  Deduplication failed: {e}")
                working_file = str(self.cleaned_dir / "cleaned_events.parquet")
            finally:
                deduplicator.close()
        else:
            working_file = str(self.cleaned_dir / "cleaned_events.parquet")

        # Optional geo-enrichment
        if enrich_geo:
            print("\n🌍 Enriching with geographic data (all coordinate sets)...")
            corrector = GeoCorrector()
            try:
                geo_output = str(self.cleaned_dir / "geo_enriched.parquet")
                geo_stats = corrector.enrich_with_reference_data(
                    working_file,
                    geo_output,
                )
                print(
                    f"✅ Geo-enriched: ActionGeo {geo_stats['action_geo']['enrichment_rate']}%, "
                    f"Actor1Geo {geo_stats['actor1_geo']['enrichment_rate']}%, "
                    f"Actor2Geo {geo_stats['actor2_geo']['enrichment_rate']}%"
                )
                stats["geo_enriched"] = geo_stats["action_geo"]["enriched_records"]
                stats["geo_enriched_actor1"] = geo_stats["actor1_geo"]["enriched_records"]
                stats["geo_enriched_actor2"] = geo_stats["actor2_geo"]["enriched_records"]
            except Exception as e:
                logger.warning(f"⚠️  Geo-enrichment failed: {e}")
            finally:
                corrector.close()

        return stats

    def step_4_scrape(
        self, limit: int = 50, enrich_geo: bool = False, deduplicate: bool = True
    ) -> dict:
        """Step 4: Scrape article content and merge with events (optional)"""
        print("\n" + "=" * 70)
        print(f"STEP 4/6: SCRAPING ARTICLES (limit: {limit})")
        print("=" * 70)

        try:
            scraper = ModernArticleScraper()
            articles_file = self.articles_dir / "enriched_articles.parquet"

            # Enable resume by passing output_file
            articles = scraper.enrich_events_with_content(
                parquet_pattern=str(self.parquet_dir / "events/**/*.parquet"),
                limit=limit,
                output_file=articles_file,  # Enable resume capability
            )

            # Save scraped articles to Parquet (final save)
            articles_scraped = 0
            if articles:
                scraper.save_enriched_articles(articles, articles_file)
                articles_scraped = len(articles)

                # Get total count including previously scraped
                import duckdb

                con = duckdb.connect(":memory:")
                total_count = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{articles_file}')"
                ).fetchone()[0]
                con.close()

                print(f"✅ Scraped {articles_scraped} new articles → {articles_file}")
                print(f"📊 Total articles in database: {total_count}")

                # Merge with geo-enriched events to create final comprehensive output
                print("\n🔗 Merging articles with geo-enriched events...")

                # Determine which events file to use (deduplicated if available)
                if deduplicate and (self.cleaned_dir / "deduplicated_events.parquet").exists():
                    base_file = str(self.cleaned_dir / "deduplicated_events.parquet")
                else:
                    base_file = str(self.cleaned_dir / "cleaned_events.parquet")

                if enrich_geo:
                    events_file = str(self.cleaned_dir / "geo_enriched.parquet")
                else:
                    events_file = base_file

                final_output = str(self.cleaned_dir / "final_enriched.parquet")

                merge_stats = scraper.merge_articles_with_events(
                    events_parquet=events_file,
                    articles_parquet=str(articles_file),
                    output_parquet=final_output,
                )

                print(
                    f"✅ Final enriched dataset: {merge_stats['events_with_articles']:,}/{merge_stats['total_events']:,} "
                    f"events ({merge_stats['enrichment_rate']}%) have article content"
                )
                print(f"📁 {final_output}")

                return merge_stats
            else:
                print("⚠️  No articles were scraped")
                return {"total_events": 0, "events_with_articles": 0, "enrichment_rate": 0.0}

        except Exception as e:
            logger.warning(f"⚠️  Scraping failed: {e}")
            logger.info("Continuing without article content...")
            import traceback

            logger.debug(traceback.format_exc())
            return {"total_events": 0, "events_with_articles": 0, "enrichment_rate": 0.0}

    def step_5_analyze(self, limit: int = 10) -> list:
        """Step 5: Quick analysis"""
        print("\n" + "=" * 70)
        print(f"STEP 5/6: ANALYZING DATA (showing top {limit})")
        print("=" * 70)

        events = query_news(
            country=self.country,
            limit=limit,
            data_dir=str(self.parquet_dir),
        )

        if events:
            print(f"\n📊 Recent {len(events)} events:")
            for i, event in enumerate(events[:5], 1):
                print(f"\n{i}. {event['SQLDATE']}")
                print(f"   {event['Actor1Name']} → {event['Actor2Name']}")
                print(f"   Location: {event['Location']}")
                print(f"   Tone: {event['AvgTone']:.2f}")
                print(f"   URL: {event['URL'][:60]}...")

        print(f"\n✅ Analysis complete - {len(events)} events analyzed")
        return events

    def run_full_pipeline(
        self,
        scrape_articles: bool = True,
        scrape_limit: int = 50,
        enrich_geo: bool = False,
        deduplicate: bool = True,
    ):
        """Run the complete pipeline"""
        self.print_banner()

        start_time = datetime.now()

        # Step 1: Collect
        results = self.step_1_collect()

        # Step 2: Validate
        metrics = self.step_2_validate()

        # Step 3: Clean + Deduplicate + Geo-enrich
        stats = self.step_3_clean(enrich_geo=enrich_geo, deduplicate=deduplicate)

        # Step 4: Scrape (optional)
        merge_stats = {"total_events": 0, "events_with_articles": 0, "enrichment_rate": 0.0}
        if scrape_articles:
            merge_stats = self.step_4_scrape(
                limit=scrape_limit, enrich_geo=enrich_geo, deduplicate=deduplicate
            )

        # Step 5: Analyze
        self.step_5_analyze()

        # Final summary
        elapsed = (datetime.now() - start_time).total_seconds()

        print("\n" + "=" * 70)
        print("🎉 PIPELINE COMPLETE")
        print("=" * 70)
        print(f"⏱️  Time elapsed: {elapsed:.1f} seconds")
        print(f"📊 Days collected: {len(results)}")
        print(f"🔢 Records cleaned: {stats['records_after']:,}")
        print(f"📝 Articles scraped: {merge_stats['events_with_articles']:,}")
        print(f"📰 Article enrichment: {merge_stats['enrichment_rate']}%")
        print(f"✨ Quality score: {metrics['quality_score']:.1f}%")
        if enrich_geo:
            print(
                f"🌍 Geo-enriched: ActionGeo {stats.get('geo_enriched', 0):,}, "
                f"Actor1Geo {stats.get('geo_enriched_actor1', 0):,}, "
                f"Actor2Geo {stats.get('geo_enriched_actor2', 0):,}"
            )
        print(f"\n📁 Output directory: {self.output_dir}")
        if scrape_articles and merge_stats["events_with_articles"] > 0:
            print(f"📄 Final enriched dataset: {self.cleaned_dir}/final_enriched.parquet")
        print("=" * 70)

        return {
            "days_collected": len(results),
            "records_cleaned": stats["records_after"],
            "articles_scraped": merge_stats["events_with_articles"],
            "article_enrichment_rate": merge_stats["enrichment_rate"],
            "quality_score": metrics["quality_score"],
            "elapsed_seconds": elapsed,
        }


def main():
    """CLI entry point"""
    import argparse

    # Smart default: Always start from 2026-01-01 to collect all available data
    default_start = "2026-01-01"

    parser = argparse.ArgumentParser(
        description="Run unified GDELT pipeline - collect, clean, dedupe, geo-enrich, scrape, analyze",
        epilog="""
Examples:
  # Full pipeline with all defaults (geo-enrichment + deduplication enabled)
  news-cn --country SA

  # Scrape unlimited articles (will take ~2-3 hours for 4000+ URLs)
  news-cn --country SA --scrape-limit 99999

  # Quick run without scraping
  news-cn --country SA --no-scrape

  # Disable geo-enrichment and deduplication (faster but less quality)
  news-cn --country SA --no-geo --no-dedupe

  # Custom date range
  news-cn --country SA --start-date 2026-01-15
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--country",
        default="SA",
        help="Country code (default: SA)",
    )
    parser.add_argument(
        "--start-date",
        default=default_start,
        help=f"Start date YYYY-MM-DD (default: {default_start} - automatically set to 2026-01-01 or today)",
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Output directory (default: data)",
    )
    parser.add_argument(
        "--strategy",
        choices=["batch", "streaming"],
        default="batch",
        help="Processing strategy (default: batch)",
    )
    parser.add_argument(
        "--no-scrape",
        action="store_true",
        help="Skip article scraping step",
    )
    parser.add_argument(
        "--scrape-limit",
        type=int,
        default=50,
        help="Max articles to scrape (default: 50, use 99999 for unlimited)",
    )
    parser.add_argument(
        "--no-geo",
        action="store_true",
        help="Disable geographic enrichment (enabled by default)",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Disable smart deduplication (enabled by default)",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Run pipeline with smart defaults
    pipeline = UnifiedPipeline(
        country=args.country,
        start_date=args.start_date,
        output_dir=args.output_dir,
        strategy=args.strategy,
    )

    pipeline.run_full_pipeline(
        scrape_articles=not args.no_scrape,
        scrape_limit=args.scrape_limit,
        enrich_geo=not args.no_geo,  # Default: ENABLED
        deduplicate=not args.no_dedupe,  # Default: ENABLED
    )


if __name__ == "__main__":
    main()
