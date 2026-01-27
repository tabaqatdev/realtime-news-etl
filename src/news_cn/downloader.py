"""
GDELT Data Downloader Module
Downloads GDELT 2.0 data files efficiently
"""

import logging
import zipfile
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GDELTDownloader:
    """Downloads GDELT 2.0 data files"""

    def __init__(self, raw_data_dir: str = "data/raw"):
        self.raw_data_dir = Path(raw_data_dir)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.master_file_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
        self.last_update_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

    def get_available_files(
        self, start_date: datetime, data_types: list[str] | None = None
    ) -> list[tuple[str, str]]:
        """
        Get list of available GDELT files from master file list

        Args:
            start_date: Only return files from this date onwards
            data_types: Types of data to download ('export', 'mentions', 'gkg')

        Returns:
            List of tuples (file_size, file_url)
        """
        if data_types is None:
            data_types = ["export", "mentions", "gkg"]

        logger.info(f"Fetching master file list from {self.master_file_url}")

        try:
            response = requests.get(self.master_file_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch master file list: {e}")
            return []

        lines = response.text.strip().split("\n")
        filtered_files = []

        for line in lines:
            parts = line.split()
            if len(parts) < 3:
                continue

            file_size, _file_hash, file_url = parts[0], parts[1], parts[2]

            # Extract date from filename (format: YYYYMMDDHHMMSS)
            # Example: 20260115000000.export.CSV.zip
            filename = Path(file_url).name

            try:
                # Extract date part (first 8 digits = YYYYMMDD)
                date_str = filename[:8]
                file_date = datetime.strptime(date_str, "%Y%m%d")

                # Filter by date
                if file_date < start_date:
                    continue

                # Filter by data type
                file_type_match = False
                for dtype in data_types:
                    if f".{dtype}." in filename.lower():
                        file_type_match = True
                        break

                if file_type_match:
                    filtered_files.append((file_size, file_url))

            except (ValueError, IndexError):
                logger.debug(f"Skipping file with unexpected format: {filename}")
                continue

        logger.info(f"Found {len(filtered_files)} files matching criteria")
        return filtered_files

    def get_latest_update(self) -> list[tuple[str, str]]:
        """
        Get the latest 15-minute update files

        Returns:
            List of tuples (file_size, file_url)
        """
        logger.info(f"Fetching latest update from {self.last_update_url}")

        try:
            response = requests.get(self.last_update_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch latest update: {e}")
            return []

        lines = response.text.strip().split("\n")
        files = []

        for line in lines:
            parts = line.split()
            if len(parts) >= 3:
                files.append((parts[0], parts[2]))

        return files

    def download_file(self, file_url: str, file_size: str | None = None) -> Path | None:
        """
        Download a single GDELT file

        Args:
            file_url: URL of the file to download
            file_size: Expected file size (for logging)

        Returns:
            Path to downloaded file, or None if failed
        """
        filename = Path(file_url).name
        output_path = self.raw_data_dir / filename

        # Skip if already downloaded
        if output_path.exists():
            logger.info(f"File already exists: {filename}")
            return output_path

        logger.info(f"Downloading {filename} (size: {file_size or 'unknown'})")

        try:
            response = requests.get(file_url, stream=True, timeout=60)
            response.raise_for_status()

            # Download with progress
            downloaded = 0

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            logger.info(f"Successfully downloaded {filename}")
            return output_path

        except requests.RequestException as e:
            logger.error(f"Failed to download {filename}: {e}")
            if output_path.exists():
                output_path.unlink()
            return None

    def download_date_range(
        self,
        start_date: datetime,
        end_date: datetime | None = None,
        data_types: list[str] | None = None,
    ) -> list[Path]:
        """
        Download all GDELT files for a date range

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive), defaults to today
            data_types: Types of data to download

        Returns:
            List of downloaded file paths
        """
        if end_date is None:
            end_date = datetime.now()

        if data_types is None:
            data_types = ["export", "mentions", "gkg"]

        logger.info(f"Downloading GDELT data from {start_date.date()} to {end_date.date()}")
        logger.info(f"Data types: {', '.join(data_types)}")

        # Get available files
        available_files = self.get_available_files(start_date, data_types)

        # Filter by end date
        filtered_files = []
        for file_size, file_url in available_files:
            filename = Path(file_url).name
            date_str = filename[:8]
            try:
                file_date = datetime.strptime(date_str, "%Y%m%d")
                if file_date <= end_date:
                    filtered_files.append((file_size, file_url))
            except ValueError:
                continue

        logger.info(f"Downloading {len(filtered_files)} files...")

        # Download files
        downloaded_paths = []
        for i, (file_size, file_url) in enumerate(filtered_files, 1):
            logger.info(f"Progress: {i}/{len(filtered_files)}")
            path = self.download_file(file_url, file_size)
            if path:
                downloaded_paths.append(path)

        logger.info(f"Download complete. Successfully downloaded {len(downloaded_paths)} files")
        return downloaded_paths

    def extract_zip(self, zip_path: Path) -> Path | None:
        """
        Extract a GDELT ZIP file

        Args:
            zip_path: Path to ZIP file

        Returns:
            Path to extracted CSV file, or None if failed
        """
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Get the first (and usually only) file in the archive
                csv_filename = zip_ref.namelist()[0]
                zip_ref.extract(csv_filename, self.raw_data_dir)
                csv_path = self.raw_data_dir / csv_filename
                logger.debug(f"Extracted {csv_filename}")
                return csv_path
        except Exception as e:
            logger.error(f"Failed to extract {zip_path}: {e}")
            return None
