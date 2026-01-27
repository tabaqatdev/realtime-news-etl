"""
Diagnostic utilities for troubleshooting
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class Diagnostics:
    """Diagnostic tools for the pipeline"""

    @staticmethod
    def check_directories(base_path: Path = None) -> dict:
        """Check all required directories"""
        if base_path is None:
            base_path = Path.cwd()

        dirs_to_check = [
            base_path / "data" / "raw",
            base_path / "data" / "parquet" / "events",
            base_path / "data" / "parquet" / "mentions",
            base_path / "data" / "parquet" / "gkg",
            base_path / "data" / "api",
        ]

        results = {}
        for d in dirs_to_check:
            exists = d.exists()
            file_count = 0
            if exists:
                files = list(d.rglob("*"))
                file_count = len([f for f in files if f.is_file()])

            results[str(d.relative_to(base_path))] = {"exists": exists, "file_count": file_count}

            if not exists:
                d.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {d.relative_to(base_path)}")

        return results

    @staticmethod
    def check_gdelt_availability(downloader, test_dates: list = None) -> dict:
        """Check GDELT file availability for various dates"""
        if test_dates is None:
            test_dates = [
                datetime.now(),
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=7),
            ]

        results = {}
        for test_date in test_dates:
            try:
                files = downloader.get_available_files(test_date, data_types=["export"])
                results[test_date.date().isoformat()] = {
                    "available": len(files) > 0,
                    "file_count": len(files) if files else 0,
                    "first_file": Path(files[0][1]).name if files else None,
                }
            except Exception as e:
                results[test_date.date().isoformat()] = {"available": False, "error": str(e)}

        return results


def main():
    """CLI entry point for diagnostics"""
    from ..downloader import GDELTDownloader

    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    print("=" * 70)
    print(" GDELT PIPELINE DIAGNOSTICS")
    print("=" * 70)

    # Check directories
    print("\n📁 Checking Directories...")
    dir_results = Diagnostics.check_directories()
    for path, info in dir_results.items():
        status = "✓" if info["exists"] else "✗"
        print(f"  {status} {path}: {info['file_count']} files")

    # Check GDELT availability
    print("\n📡 Checking GDELT Data Availability...")
    downloader = GDELTDownloader()
    gdelt_results = Diagnostics.check_gdelt_availability(downloader)

    for date_str, info in gdelt_results.items():
        if info.get("available"):
            print(f"  ✓ {date_str}: {info['file_count']} files")
            if info.get("first_file"):
                print(f"     Example: {info['first_file']}")
        else:
            error_msg = info.get("error", "No files found")
            print(f"  ✗ {date_str}: {error_msg}")

    print("\n" + "=" * 70)
    print("✅ Diagnostics complete!")
    print("\n💡 Next step: uv run news-cn")


if __name__ == "__main__":
    main()
