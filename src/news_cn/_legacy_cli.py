"""
CLI Module - Command Line Interface
Modern Python CLI using click or argparse
"""

import logging
from datetime import datetime
from pathlib import Path

from .config import Config
from .consolidator import DataConsolidator
from .downloader import GDELTDownloader
from .efficient_processor import EfficientGDELTProcessor
from .state_manager import StateManager

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class GDELTPipeline:
    """Main pipeline orchestrator"""

    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self.downloader = GDELTDownloader(raw_data_dir=str(self.config.RAW_DATA_DIR))
        self.processor = EfficientGDELTProcessor(
            output_dir=str(self.config.PARQUET_OUTPUT_DIR),
            memory_limit=self.config.DUCKDB_MEMORY_LIMIT,
            threads=self.config.DUCKDB_THREADS,
        )
        self.state_manager = StateManager()
        self.consolidator = DataConsolidator(self.config.PARQUET_OUTPUT_DIR)

    def print_banner(self):
        """Print welcome banner"""
        banner = """
╔═══════════════════════════════════════════════════════════╗
║   GDELT Saudi Arabia Data Pipeline                       ║
║   100% FREE - No API keys required!                      ║
║   Collecting 2026+ data with automatic incremental       ║
║   processing and daily consolidation                     ║
╚═══════════════════════════════════════════════════════════╝
        """
        print(banner)

    def clean_raw_directory(self):
        """Clean raw data directory to save space"""
        if not self.config.CLEAN_RAW_DIR_ON_START:
            return

        if self.config.RAW_DATA_DIR.exists():
            import shutil

            file_count = len(list(self.config.RAW_DATA_DIR.rglob("*")))
            if file_count > 0:
                logger.info(f"🧹 Cleaning raw directory ({file_count} files)...")
                shutil.rmtree(self.config.RAW_DATA_DIR)
                self.config.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
                logger.info("✓ Raw directory cleaned")

    def run(self):
        """Execute the full pipeline"""
        self.print_banner()

        # Clean raw directory if enabled
        self.clean_raw_directory()

        now = datetime.now()

        logger.info(
            f"🎯 Target: {self.config.TARGET_COUNTRY_NAME} ({self.config.TARGET_COUNTRY_CODE})"
        )
        logger.info(f"📅 Date range: {self.config.START_DATE.date()} → {now.date()}")

        # Show current state
        stats = self.state_manager.get_stats()
        if stats["successful"] > 0:
            logger.info(f"📊 Previously processed: {stats['successful']} files")
            logger.info(f"📦 Consolidated days: {stats['consolidated_days']}")
            logger.info("🔄 Running incremental update (skipping processed files)...")

        # Execute streaming download for all data
        self._run_streaming_download(
            start_date=self.config.START_DATE, end_date=now, data_types=self.config.DATA_TYPES
        )

        # Consolidate completed days
        if self.config.AUTO_CONSOLIDATE:
            logger.info("\n" + "=" * 70)
            logger.info("CONSOLIDATING DAILY DATA")
            logger.info("=" * 70)
            self._consolidate_completed_days()

        logger.info("\n✅ Pipeline execution complete!")
        logger.info(f"📁 Check your data in: {self.config.PARQUET_OUTPUT_DIR}")

        # Show final stats
        final_stats = self.state_manager.get_stats()
        logger.info("\n📊 Final Stats:")
        logger.info(f"   Total files processed: {final_stats['total_files_processed']}")
        logger.info(f"   Successful: {final_stats['successful']}")
        logger.info(f"   Consolidated days: {final_stats['consolidated_days']}")

    def _run_streaming_download(self, start_date: datetime, end_date: datetime, data_types: list):
        """Run streaming download and processing"""
        logger.info("\n" + "=" * 70)
        logger.info("STREAMING DOWNLOAD + FILTERING")
        logger.info("=" * 70)

        logger.info(f"📅 Date range: {start_date.date()} to {end_date.date()}")
        logger.info(f"📦 Data types: {', '.join(data_types)}")

        # Get list of files to download
        available_files = self.downloader.get_available_files(start_date, data_types)

        # Filter by end date
        filtered_files = []
        for file_size, file_url in available_files:
            filename = Path(file_url).name
            date_str = filename[:8]
            try:
                file_date = datetime.strptime(date_str, "%Y%m%d")
                if start_date <= file_date <= end_date:
                    filtered_files.append((file_size, file_url))
            except ValueError:
                continue

        logger.info(f"📊 Found {len(filtered_files)} files to process")

        if not filtered_files:
            logger.warning("⚠ No files found for the specified date range")
            return

        # Process files and consolidate each day as it completes
        successful = 0
        skipped = 0
        failed = 0
        current_day = None
        days_consolidated_inline = set()

        for i, (_file_size, file_url) in enumerate(filtered_files, 1):
            filename_only = Path(file_url).name

            # Extract date from filename
            file_day = filename_only[:8] if len(filename_only) >= 8 else None

            # Skip if already processed
            if self.state_manager.is_file_processed(filename_only):
                logger.debug(
                    f"[{i}/{len(filtered_files)}] Skipping (already processed): {filename_only}"
                )
                skipped += 1
                continue

            logger.info(f"\n[{i}/{len(filtered_files)}] Processing: {filename_only}")

            try:
                filename = filename_only.lower()
                result = None

                if ".export." in filename:
                    result = self.processor.stream_and_filter_events(
                        file_url, self.config.TARGET_COUNTRY_CODE
                    )
                elif ".mentions." in filename:
                    result = self.processor.stream_and_filter_mentions(file_url)
                elif ".gkg." in filename:
                    result = self.processor.stream_and_filter_gkg(
                        file_url, self.config.TARGET_COUNTRY_CODE
                    )

                if result:
                    successful += 1
                    self.state_manager.mark_file_processed(filename_only, "success")
                else:
                    skipped += 1
                    self.state_manager.mark_file_processed(filename_only, "no_data")

            except Exception as e:
                logger.error(f"✗ Error processing file: {e}")
                failed += 1
                self.state_manager.mark_file_processed(filename_only, "failed")

            # Check if we've moved to a new day - consolidate the previous day
            if (
                file_day
                and current_day
                and file_day != current_day
                and self.config.AUTO_CONSOLIDATE
                and current_day not in days_consolidated_inline
            ):
                logger.info(f"\n📦 Day complete - consolidating {current_day}...")
                if self._consolidate_single_day(current_day):
                    days_consolidated_inline.add(current_day)

            current_day = file_day

        # Consolidate the last day if enabled
        if (
            current_day
            and self.config.AUTO_CONSOLIDATE
            and current_day not in days_consolidated_inline
        ):
            logger.info(f"\n📦 Consolidating final day {current_day}...")
            self._consolidate_single_day(current_day)

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("PROCESSING COMPLETE")
        logger.info("=" * 70)
        logger.info(f"✓ Successful: {successful}")
        logger.info(f"○ Skipped (no SA data): {skipped}")
        logger.info(f"✗ Failed: {failed}")

        self.processor.close()

    def _consolidate_single_day(self, date_str: str) -> bool:
        """
        Consolidate a single day's files

        Args:
            date_str: Date string in YYYYMMDD format

        Returns:
            True if consolidation succeeded, False otherwise
        """
        # Skip if already consolidated
        if self.state_manager.is_day_consolidated(date_str):
            logger.debug(f"Date {date_str} already consolidated")
            return False

        # Check if this is a "complete" day (not today)
        try:
            file_date = datetime.strptime(date_str, "%Y%m%d")
            if file_date.date() >= datetime.now().date():
                logger.debug(f"Skipping {date_str} (current day, not complete)")
                return False
        except ValueError:
            return False

        # Consolidate this day
        result = self.consolidator.consolidate_day(
            date_str, data_type="events", delete_originals=True
        )
        if result:
            self.state_manager.mark_day_consolidated(date_str)
            logger.info(f"✓ Consolidated {date_str}: {result['files_merged']} files → 1 file")
            return True

        return False

    def _consolidate_completed_days(self):
        """Consolidate any remaining days that weren't consolidated inline"""
        # Get all unique dates from processed files
        dates_to_check = set()
        for filename in self.state_manager.state["processed_files"]:
            if len(filename) >= 8:
                date_str = filename[:8]
                dates_to_check.add(date_str)

        if not dates_to_check:
            logger.info("No dates to consolidate")
            return

        logger.info(f"Checking {len(dates_to_check)} dates for consolidation...")

        consolidated_count = 0
        for date_str in sorted(dates_to_check):
            if self._consolidate_single_day(date_str):
                consolidated_count += 1

        if consolidated_count > 0:
            logger.info(f"\n✓ Consolidated {consolidated_count} remaining days")
        else:
            logger.info("No new days to consolidate")


def main():
    """CLI entry point"""
    pipeline = GDELTPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
