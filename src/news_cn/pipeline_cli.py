"""
Unified Pipeline CLI - Run complete GDELT pipeline with one command
Orchestrates: Download → Process → Clean → Scrape → Analyze

Modes:
  --mode full   : Process all days into one combined file (legacy)
  --mode daily  : Per-day output files with gap-filling (recommended)
"""

import logging
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from .data_cleaner import clean_events_data, validate_data_quality
from .deduplicator import SmartDeduplicator
from .downloader import GDELTDownloader
from .geo_corrector import GeoCorrector
from .modern_scraper import ModernArticleScraper
from .schemas import SchemaFactory
from .simple import collect_news, query_news

DEFAULT_INTERVAL_COUNTRIES = "SA,AE,QA,KW,BH,OM,IS"

logger = logging.getLogger(__name__)


class UnifiedPipeline:
    """Complete GDELT pipeline orchestrator"""

    def __init__(
        self,
        country: str = "SA",
        start_date: str = "2026-01-01",
        end_date: str | None = None,
        output_dir: str = "data",
        strategy: str = "batch",
    ):
        self.country = country
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = Path(output_dir)
        self.strategy = strategy

        # Paths
        self.parquet_dir = self.output_dir / "parquet"
        self.articles_dir = self.output_dir / "articles"
        self.cleaned_dir = self.output_dir / "parquet" / "cleaned"

    def print_banner(self, mode: str = "full"):
        end_display = self.end_date or "today"
        banner = f"""
╔══════════════════════════════════════════════════════════════════════╗
║             GDELT UNIFIED PIPELINE - {mode.upper():^10}                      ║
║  Collect → Process → Clean → Dedupe → Geo → Scrape → Analyze        ║
╚══════════════════════════════════════════════════════════════════════╝

📍 Country: {self.country}
📅 Date Range: {self.start_date} → {end_display}
📁 Output: {self.output_dir}
⚡ Strategy: {self.strategy}
"""
        print(banner)

    # ─── Daily Mode (per-day output files) ────────────────────────

    def run_daily_pipeline(self, scrape_limit: int = 500):
        """
        Process each day independently into separate parquet files.
        Skips days that already have output files (idempotent gap-filling).

        Output structure:
          data/output/country=SA/year=2026/2026_01_15.parquet
        """
        self.print_banner(mode="daily")
        start_time = datetime.now()

        # 1. Determine date range
        start = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        end = (
            datetime.strptime(self.end_date, "%Y-%m-%d").date()
            if self.end_date
            else date.today() - timedelta(days=1)
        )

        # 2. Output directory
        output_base = self.output_dir / "output" / f"country={self.country}"

        # 3. Find which days already have output
        existing = set()
        if output_base.exists():
            for f in output_base.rglob("*.parquet"):
                existing.add(f.stem)  # "2026_01_15"

        # 4. Build list of missing days
        all_days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        missing = [d for d in all_days if d.strftime("%Y_%m_%d") not in existing]

        if not missing:
            print(f"✅ All {len(all_days)} days already processed!")
            return

        print(f"📊 Total days in range: {len(all_days)}")
        print(f"✅ Already processed: {len(existing)}")
        print(f"🔄 Missing (to process): {len(missing)}")

        # 5. Collect GDELT data for the missing date range
        collect_start = min(missing).strftime("%Y-%m-%d")
        collect_end = max(missing).strftime("%Y-%m-%d")

        print("\n" + "=" * 70)
        print("STEP 1: COLLECTING GDELT DATA")
        print("=" * 70)

        collect_news(
            country=self.country,
            start_date=collect_start,
            end_date=collect_end,
            output_dir=str(self.parquet_dir),
            strategy=self.strategy,
        )

        # 6. Process each missing day
        processed = 0
        for idx, day in enumerate(missing, 1):
            print(f"\n{'=' * 70}")
            print(f"[Day {idx}/{len(missing)}] Processing {day.isoformat()}")
            print("=" * 70)

            try:
                output_file = self._process_single_day(day, output_base, scrape_limit)
                if output_file:
                    processed += 1
                    print(f"  ✅ → {output_file}")
            except Exception as e:
                logger.error(f"  ✗ Failed to process {day}: {e}")
                continue

        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n{'=' * 70}")
        print("🎉 DAILY PIPELINE COMPLETE")
        print("=" * 70)
        print(f"⏱️  Time elapsed: {elapsed:.1f} seconds")
        print(f"📊 Days processed: {processed}/{len(missing)}")
        print(f"📁 Output: {output_base}/")
        print("=" * 70)

    def _process_single_day(self, day: date, output_base: Path, scrape_limit: int) -> Path | None:
        """
        Full pipeline for one day:
        raw parquet → clean → dedupe → geo-enrich → scrape → merge → daily output file
        """
        date_str = day.strftime("%Y%m%d")
        day_num = f"{day.day:02d}"
        month_num = f"{day.month:02d}"

        # Find this day's consolidated parquet
        day_pattern = str(
            self.parquet_dir
            / "events"
            / f"year={day.year}"
            / f"month={month_num}"
            / f"day={day_num}"
            / "*.parquet"
        )

        con = duckdb.connect(":memory:")
        try:
            count = con.execute(f"SELECT count(*) FROM read_parquet('{day_pattern}')").fetchone()[0]
        except Exception:
            logger.warning(f"  No raw data found for {day.isoformat()}")
            con.close()
            return None

        if count == 0:
            logger.warning(f"  No records for {day.isoformat()}")
            con.close()
            return None

        print(f"  📊 Raw records: {count:,}")

        # Use temp dir for intermediate files
        tmp = self.output_dir / "tmp" / date_str
        tmp.mkdir(parents=True, exist_ok=True)

        try:
            # Step 1: Clean
            cleaned_file = tmp / "cleaned.parquet"
            con.execute(f"""
                COPY (
                    SELECT DISTINCT *
                    FROM read_parquet('{day_pattern}')
                    WHERE GLOBALEVENTID IS NOT NULL
                      AND SQLDATE IS NOT NULL
                      AND SOURCEURL IS NOT NULL AND SOURCEURL != ''
                      AND AvgTone BETWEEN -10.0 AND 10.0
                    ORDER BY SQLDATE DESC, DATEADDED DESC
                ) TO '{cleaned_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            cleaned_count = con.execute(
                f"SELECT count(*) FROM read_parquet('{cleaned_file}')"
            ).fetchone()[0]
            print(f"  🧹 Cleaned: {cleaned_count:,}")

            # Step 2: Deduplicate (best record per URL)
            deduped_file = tmp / "deduped.parquet"
            deduplicator = SmartDeduplicator()
            try:
                deduplicator.deduplicate_by_url(str(cleaned_file), str(deduped_file))
                deduped_count = con.execute(
                    f"SELECT count(*) FROM read_parquet('{deduped_file}')"
                ).fetchone()[0]
                print(f"  🔍 Deduplicated: {deduped_count:,} unique URLs")
            finally:
                deduplicator.close()

            # Step 3: Geo-enrich
            geo_file = tmp / "geo.parquet"
            corrector = GeoCorrector()
            try:
                corrector.enrich_with_reference_data(str(deduped_file), str(geo_file))
                print("  🌍 Geo-enriched")
            except Exception as e:
                logger.warning(f"  ⚠️  Geo-enrichment failed: {e}, using deduped file")
                geo_file = deduped_file
            finally:
                corrector.close()

            # Step 4: Scrape articles
            articles_file = tmp / "articles.parquet"
            scraper = ModernArticleScraper()
            scraper.enrich_events_with_content(
                parquet_pattern=str(geo_file),
                limit=scrape_limit,
                output_file=articles_file,
                events_file=str(geo_file),
                final_output_file=None,  # We'll merge ourselves
            )

            # Step 5: Merge events + articles (LEFT JOIN)
            year_dir = output_base / f"year={day.year}"
            year_dir.mkdir(parents=True, exist_ok=True)
            output_file = year_dir / f"{day.strftime('%Y_%m_%d')}.parquet"

            if articles_file.exists():
                scraper.merge_articles_with_events(
                    events_parquet=str(geo_file),
                    articles_parquet=str(articles_file),
                    output_parquet=str(output_file),
                    only_with_articles=False,
                )
            else:
                # No articles scraped — copy geo-enriched as final output with NULL article cols
                con.execute(f"""
                    COPY (
                        SELECT *,
                            NULL::VARCHAR as ArticleTitle,
                            NULL::VARCHAR as ArticleContent,
                            NULL::VARCHAR as ArticleAuthor,
                            NULL::VARCHAR as ArticlePublishDate,
                            NULL::BIGINT as ArticleContentLength,
                            NULL::VARCHAR as ArticleScrapeMethod
                        FROM read_parquet('{geo_file}')
                    ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)

            # Verify
            stats = con.execute(f"""
                SELECT count(*) as total, count(ArticleTitle) as with_articles
                FROM read_parquet('{output_file}')
            """).fetchone()
            print(f"  📰 Final: {stats[0]:,} events, {stats[1]:,} with articles")

            return output_file

        finally:
            con.close()
            # Clean up temp files
            shutil.rmtree(tmp, ignore_errors=True)

    # ─── Interval Mode (latest 15-min update → GeoPackage) ───────

    def run_interval_pipeline(self, retention_days: int = 7):
        """
        Fetch the latest GDELT 15-minute update, run ETL (no scraping),
        and maintain a rolling GeoPackage with the last N days of data.

        Supports multiple countries via self.country (comma-separated codes).
        Output: data/output/events.gpkg
        """
        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import Point

        start_time = datetime.now()
        countries = [c.strip() for c in self.country.split(",")]
        country_list_sql = ", ".join(f"'{c}'" for c in countries)

        print(f"\n{'=' * 70}")
        print("GDELT INTERVAL PIPELINE — Latest 15-min update")
        print(f"{'=' * 70}")
        print(f"  Countries: {', '.join(countries)}")
        print(f"  Retention: {retention_days} days")

        # 1. Get latest export update
        downloader = GDELTDownloader(raw_data_dir=str(self.output_dir / "raw"))
        update = downloader.get_latest_export_update()
        if update is None:
            print("  No export update available. Exiting.")
            return
        timestamp, url = update
        print(f"  Update: {timestamp}")

        # 2. Download and extract
        csv_path = downloader.download_and_extract(url)
        if csv_path is None:
            print("  Failed to download/extract. Exiting.")
            return

        # 3. Read CSV, filter by country codes
        tmp = self.output_dir / "tmp" / f"interval_{timestamp}"
        tmp.mkdir(parents=True, exist_ok=True)

        schema = SchemaFactory.get_event_schema()
        col_defs = schema.to_duckdb_string()

        con = duckdb.connect(":memory:")
        try:
            raw_file = tmp / "raw.parquet"
            con.execute(f"""
                COPY (
                    SELECT *
                    FROM read_csv('{csv_path}',
                        delim='\t', header=false, quote='',
                        columns={{{col_defs}}})
                    WHERE ActionGeo_CountryCode IN ({country_list_sql})
                       OR Actor1Geo_CountryCode IN ({country_list_sql})
                       OR Actor2Geo_CountryCode IN ({country_list_sql})
                ) TO '{raw_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            raw_count = con.execute(f"SELECT count(*) FROM read_parquet('{raw_file}')").fetchone()[
                0
            ]
            print(f"  Raw records (filtered): {raw_count:,}")

            if raw_count == 0:
                print("  No matching records. Exiting.")
                return

            # 4. Clean
            cleaned_file = tmp / "cleaned.parquet"
            con.execute(f"""
                COPY (
                    SELECT DISTINCT *
                    FROM read_parquet('{raw_file}')
                    WHERE GLOBALEVENTID IS NOT NULL
                      AND SQLDATE IS NOT NULL
                      AND SOURCEURL IS NOT NULL AND SOURCEURL != ''
                      AND AvgTone BETWEEN -10.0 AND 10.0
                    ORDER BY SQLDATE DESC, DATEADDED DESC
                ) TO '{cleaned_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            cleaned_count = con.execute(
                f"SELECT count(*) FROM read_parquet('{cleaned_file}')"
            ).fetchone()[0]
            print(f"  Cleaned: {cleaned_count:,}")

            # 5. Deduplicate
            deduped_file = tmp / "deduped.parquet"
            deduplicator = SmartDeduplicator()
            try:
                deduplicator.deduplicate_by_url(str(cleaned_file), str(deduped_file))
                deduped_count = con.execute(
                    f"SELECT count(*) FROM read_parquet('{deduped_file}')"
                ).fetchone()[0]
                print(f"  Deduplicated: {deduped_count:,}")
            finally:
                deduplicator.close()

            # 6. Geo-enrich
            geo_file = tmp / "geo.parquet"
            corrector = GeoCorrector()
            try:
                corrector.enrich_with_reference_data(str(deduped_file), str(geo_file))
                print("  Geo-enriched")
            except Exception as e:
                logger.warning(f"  Geo-enrichment failed: {e}, using deduped file")
                geo_file = deduped_file
            finally:
                corrector.close()

            # 7. Scrape articles
            articles_file = tmp / "articles.parquet"
            scraper = ModernArticleScraper()
            scraper.enrich_events_with_content(
                parquet_pattern=str(geo_file),
                limit=500,
                output_file=articles_file,
                events_file=str(geo_file),
                final_output_file=None,
            )

            # 8. Merge events + articles (LEFT JOIN)
            final_file = tmp / "final.parquet"
            if articles_file.exists():
                scraper.merge_articles_with_events(
                    events_parquet=str(geo_file),
                    articles_parquet=str(articles_file),
                    output_parquet=str(final_file),
                    only_with_articles=False,
                )
            else:
                # No articles scraped — add NULL article columns
                con.execute(f"""
                    COPY (
                        SELECT *,
                            NULL::VARCHAR as ArticleTitle,
                            NULL::VARCHAR as ArticleContent,
                            NULL::VARCHAR as ArticleAuthor,
                            NULL::VARCHAR as ArticlePublishDate,
                            NULL::BIGINT as ArticleContentLength,
                            NULL::VARCHAR as ArticleScrapeMethod
                        FROM read_parquet('{geo_file}')
                    ) TO '{final_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)

            source_file = final_file if final_file.exists() else geo_file
            stats = con.execute(f"""
                SELECT count(*) as total, count(ArticleTitle) as with_articles
                FROM read_parquet('{source_file}')
            """).fetchone()
            print(f"  Articles: {stats[1]:,}/{stats[0]:,} events with content")

            # 9. Convert to GeoDataFrame
            df = pd.read_parquet(str(source_file))
            # Only create geometry where we have coordinates
            has_coords = df["ActionGeo_Lat"].notna() & df["ActionGeo_Long"].notna()
            geometry = [
                Point(row["ActionGeo_Long"], row["ActionGeo_Lat"]) if has else None
                for has, (_, row) in zip(has_coords, df.iterrows(), strict=False)
            ]
            gdf_new = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            print(f"  GeoDataFrame: {len(gdf_new):,} records, {has_coords.sum():,} with geometry")

            # 10. Append to GeoPackage with rolling window
            gpkg_path = self.output_dir / "output" / "events.gpkg"
            gpkg_path.parent.mkdir(parents=True, exist_ok=True)

            cutoff_date = int((date.today() - timedelta(days=retention_days)).strftime("%Y%m%d"))

            if gpkg_path.exists():
                gdf_existing = gpd.read_file(gpkg_path, layer="events")
                print(f"  Existing GeoPackage: {len(gdf_existing):,} records")

                # Concat and deduplicate by GLOBALEVENTID
                gdf_combined = pd.concat([gdf_existing, gdf_new], ignore_index=True)
                gdf_combined = gdf_combined.drop_duplicates(subset=["GLOBALEVENTID"], keep="last")

                # Prune old records
                gdf_combined = gdf_combined[gdf_combined["SQLDATE"] >= cutoff_date]
                gdf_combined = gpd.GeoDataFrame(gdf_combined, geometry="geometry", crs="EPSG:4326")
            else:
                gdf_combined = gdf_new[gdf_new["SQLDATE"] >= cutoff_date]

            gdf_combined.to_file(str(gpkg_path), driver="GPKG", layer="events")
            print(f"  GeoPackage written: {len(gdf_combined):,} records → {gpkg_path}")

        finally:
            con.close()
            # Clean up temp files and downloaded CSV
            shutil.rmtree(tmp, ignore_errors=True)
            if csv_path.exists():
                csv_path.unlink(missing_ok=True)

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n  Done in {elapsed:.1f}s")
        print(f"{'=' * 70}")

    # ─── Full Mode (legacy: one combined file) ────────────────────

    def step_1_collect(self) -> dict:
        """Step 1: Collect GDELT data"""
        print("\n" + "=" * 70)
        print("STEP 1/6: COLLECTING GDELT DATA")
        print("=" * 70)

        results = collect_news(
            country=self.country,
            start_date=self.start_date,
            end_date=self.end_date,
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

                working_file = dedup_output
            except Exception as e:
                logger.warning(f"⚠️  Deduplication failed: {e}")
                working_file = str(self.cleaned_dir / "cleaned_events.parquet")
            finally:
                deduplicator.close()
        else:
            working_file = str(self.cleaned_dir / "cleaned_events.parquet")

        if enrich_geo:
            print("\n🌍 Enriching with geographic data (all coordinate sets)...")
            corrector = GeoCorrector()
            try:
                geo_output = str(self.cleaned_dir / "geo_enriched.parquet")
                geo_stats = corrector.enrich_with_reference_data(working_file, geo_output)
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
        """Step 4: Scrape article content and merge with events"""
        print("\n" + "=" * 70)
        print(f"STEP 4/6: SCRAPING ARTICLES (limit: {limit})")
        print("=" * 70)

        try:
            scraper = ModernArticleScraper()
            articles_file = self.articles_dir / "enriched_articles.parquet"

            if deduplicate and (self.cleaned_dir / "deduplicated_events.parquet").exists():
                base_file = str(self.cleaned_dir / "deduplicated_events.parquet")
            else:
                base_file = str(self.cleaned_dir / "cleaned_events.parquet")

            if enrich_geo:
                events_file = str(self.cleaned_dir / "geo_enriched.parquet")
            else:
                events_file = base_file

            final_output = self.cleaned_dir / "final_enriched.parquet"

            articles = scraper.enrich_events_with_content(
                parquet_pattern=str(self.parquet_dir / "events/**/*.parquet"),
                limit=limit,
                output_file=articles_file,
                events_file=events_file,
                final_output_file=final_output,
            )

            articles_scraped = len(articles) if articles else 0

            if articles_file.exists():
                con = duckdb.connect(":memory:")
                total_count = con.execute(
                    f"SELECT COUNT(DISTINCT url) FROM read_parquet('{articles_file}')"
                ).fetchone()[0]
                con.close()

                print(f"✅ Scraped {articles_scraped} new articles → {articles_file}")
                print(f"📊 Total unique articles in database: {total_count}")

                print("\n🔗 Final merge with events...")
                merge_stats = scraper.merge_articles_with_events(
                    events_parquet=events_file,
                    articles_parquet=str(articles_file),
                    output_parquet=str(final_output),
                )

                print(
                    f"✅ Final enriched dataset: {merge_stats['events_with_articles']:,}"
                    f"/{merge_stats['total_events']:,} "
                    f"events ({merge_stats['enrichment_rate']}%) have article content"
                )
                print(f"📁 {final_output}")

                return merge_stats
            else:
                print("⚠️  No articles were scraped")
                return {"total_events": 0, "events_with_articles": 0, "enrichment_rate": 0.0}

        except Exception as e:
            logger.warning(f"⚠️  Scraping failed: {e}")
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
        """Run the complete pipeline (full mode: one combined output file)"""
        self.print_banner(mode="full")

        start_time = datetime.now()

        results = self.step_1_collect()
        metrics = self.step_2_validate()
        stats = self.step_3_clean(enrich_geo=enrich_geo, deduplicate=deduplicate)

        merge_stats = {"total_events": 0, "events_with_articles": 0, "enrichment_rate": 0.0}
        if scrape_articles:
            merge_stats = self.step_4_scrape(
                limit=scrape_limit, enrich_geo=enrich_geo, deduplicate=deduplicate
            )

        self.step_5_analyze()

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


def main():
    """CLI entry point"""
    import argparse

    default_start = "2026-01-01"

    parser = argparse.ArgumentParser(
        description="GDELT news pipeline - collect, clean, dedupe, geo-enrich, scrape, analyze",
        epilog="""
Examples:
  # Daily mode: process yesterday (default for CI/cron)
  news-cn --mode daily

  # Daily mode: backfill from Jan 1 to Feb 7
  news-cn --mode daily --start-date 2026-01-01 --end-date 2026-02-07

  # Interval mode: latest 15-min update for Gulf + Israel → GeoPackage
  news-cn --mode interval

  # Interval mode: custom countries and retention
  news-cn --mode interval --country SA,AE,QA --retention-days 14

  # Full mode (legacy): all data into one file
  news-cn --mode full --scrape-limit 99999

  # Quick run without scraping
  news-cn --mode full --no-scrape
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--country",
        default=None,
        help="Country code(s), comma-separated (default: SA for daily/full, "
        "SA,AE,QA,KW,BH,OM,IS for interval)",
    )
    parser.add_argument(
        "--start-date",
        default=default_start,
        help=f"Start date YYYY-MM-DD (default: {default_start})",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date YYYY-MM-DD (inclusive). In daily mode defaults to yesterday.",
    )
    parser.add_argument("--output-dir", default="data", help="Output directory (default: data)")
    parser.add_argument(
        "--strategy",
        choices=["batch", "streaming"],
        default="batch",
        help="Processing strategy (default: batch)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "daily", "interval"],
        default="daily",
        help="Pipeline mode: 'daily' (per-day files), 'interval' (15-min update → GeoPackage), "
        "or 'full' (one combined file)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Days of data to keep in GeoPackage (interval mode, default: 7)",
    )
    parser.add_argument("--no-scrape", action="store_true", help="Skip article scraping step")
    parser.add_argument(
        "--scrape-limit",
        type=int,
        default=500,
        help="Max articles to scrape per day (default: 500)",
    )
    parser.add_argument("--no-geo", action="store_true", help="Disable geographic enrichment")
    parser.add_argument("--no-dedupe", action="store_true", help="Disable smart deduplication")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Resolve country default based on mode
    if args.country is None:
        country = DEFAULT_INTERVAL_COUNTRIES if args.mode == "interval" else "SA"
    else:
        country = args.country

    pipeline = UnifiedPipeline(
        country=country,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        strategy=args.strategy,
    )

    if args.mode == "interval":
        pipeline.run_interval_pipeline(retention_days=args.retention_days)
    elif args.mode == "daily":
        pipeline.run_daily_pipeline(scrape_limit=args.scrape_limit)
    else:
        pipeline.run_full_pipeline(
            scrape_articles=not args.no_scrape,
            scrape_limit=args.scrape_limit,
            enrich_geo=not args.no_geo,
            deduplicate=not args.no_dedupe,
        )


if __name__ == "__main__":
    main()
