"""
GDELT API Client
100% FREE - No API keys required!
Uses GDELT DOC and GEO APIs to get pre-filtered data
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GDELTAPIClient:
    """
    Client for GDELT's free JSON APIs
    No authentication required!
    """

    def __init__(self, output_dir: str = "data/api"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # API endpoints (all free!)
        self.doc_api = "https://api.gdeltproject.org/api/v2/doc/doc"
        self.geo_api = "https://api.gdeltproject.org/api/v2/geo/geo"
        self.tv_api = "https://api.gdeltproject.org/api/v2/tv/tv"

    def query_doc_api(
        self,
        query: str = "*",
        country: str = "SA",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        mode: str = "artlist",
        max_records: int = 250,
    ) -> list[dict]:
        """
        Query GDELT DOC API for articles from Saudi Arabia

        API Docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/

        Args:
            query: Search query (use "*" for all articles)
            country: FIPS country code (SA for Saudi Arabia)
            start_date: Start date (defaults to 3 months ago - API limit)
            end_date: End date (defaults to now)
            mode: 'artlist' for article list, 'timeline' for timeline
            max_records: Max records to return (default 250, max 250 per request)

        Returns:
            List of article dictionaries
        """
        if start_date is None:
            # API supports last 3 months
            start_date = datetime.now() - timedelta(days=90)

        if end_date is None:
            end_date = datetime.now()

        logger.info(f"📡 Querying DOC API for {country}: {start_date.date()} to {end_date.date()}")

        params = {
            "query": query,
            "mode": mode,
            "maxrecords": max_records,
            "format": "json",
            "sourcecountry": country,  # Filter by source country
            "startdatetime": start_date.strftime("%Y%m%d%H%M%S"),
            "enddatetime": end_date.strftime("%Y%m%d%H%M%S"),
        }

        try:
            response = requests.get(self.doc_api, params=params, timeout=30)
            response.raise_for_status()

            # Check if response is empty or invalid
            if not response.text or len(response.text.strip()) == 0:
                logger.warning("⚠ Empty response from API")
                return []

            data = response.json()

            if "articles" in data:
                articles = data["articles"]
                logger.info(f"✓ Retrieved {len(articles)} articles from {country}")
                return articles
            else:
                logger.warning(
                    f"⚠ No 'articles' key in response. Response keys: {list(data.keys())}"
                )
                return []

        except requests.Timeout as e:
            logger.error(f"✗ API request timed out: {e}")
            logger.info(
                "💡 Tip: GDELT APIs can be slow. Try again or use the streaming download method instead."
            )
            return []
        except requests.RequestException as e:
            logger.error(f"✗ API request failed: {e}")
            logger.info(
                "💡 Tip: Check your internet connection or try the streaming download method."
            )
            return []
        except json.JSONDecodeError as e:
            logger.error(f"✗ Failed to parse JSON response: {e}")
            logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
            logger.info(
                "💡 Tip: GDELT API might be temporarily unavailable. Try the streaming download method instead."
            )
            return []

    def query_geo_api(
        self,
        query: str = "*",
        country: str = "SA",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        mode: str = "pointdata",
        max_records: int = 250,
    ) -> dict:
        """
        Query GDELT GEO API for geographic data from Saudi Arabia

        API Docs: https://blog.gdeltproject.org/gdelt-geo-2-0-api-debuts/

        Args:
            query: Search query (use "*" for all)
            country: Country name or code
            start_date: Start date
            end_date: End date
            mode: 'pointdata' for individual points, 'heatmap' for heatmap
            max_records: Max records to return

        Returns:
            Dictionary with geographic data
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=90)

        if end_date is None:
            end_date = datetime.now()

        logger.info(f"📍 Querying GEO API for {country}: {start_date.date()} to {end_date.date()}")

        params = {
            "query": query,
            "mode": mode,
            "maxrecords": max_records,
            "format": "json",
            "geoloc": country,  # Filter by location
            "startdatetime": start_date.strftime("%Y%m%d%H%M%S"),
            "enddatetime": end_date.strftime("%Y%m%d%H%M%S"),
        }

        try:
            response = requests.get(self.geo_api, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if "data" in data:
                logger.info(f"✓ Retrieved geographic data from {country}")
                return data
            else:
                logger.warning("No geographic data found")
                return {}

        except requests.RequestException as e:
            logger.error(f"✗ API request failed: {e}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"✗ Failed to parse JSON response: {e}")
            return {}

    def get_recent_saudi_news(self, days_back: int = 7) -> list[dict]:
        """
        Quick method to get recent news from Saudi Arabia

        Args:
            days_back: Number of days to look back (max 90)

        Returns:
            List of articles
        """
        if days_back > 90:
            logger.warning("API supports max 90 days, limiting to 90")
            days_back = 90

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        return self.query_doc_api(
            query="*", country="SA", start_date=start_date, end_date=end_date, max_records=250
        )

    def get_saudi_events_by_date_range(
        self, start_date: datetime, end_date: datetime, batch_size_days: int = 7
    ) -> list[dict]:
        """
        Get Saudi Arabia events for a date range using batched API calls
        (API has limits, so we break into smaller chunks)

        Args:
            start_date: Start date
            end_date: End date
            batch_size_days: Days per batch (smaller = more API calls but more complete data)

        Returns:
            Combined list of all articles
        """
        all_articles = []
        current_date = start_date

        while current_date < end_date:
            batch_end = min(current_date + timedelta(days=batch_size_days), end_date)

            logger.info(f"📅 Fetching batch: {current_date.date()} to {batch_end.date()}")

            articles = self.query_doc_api(
                query="*",
                country="SA",
                start_date=current_date,
                end_date=batch_end,
                max_records=250,
            )

            all_articles.extend(articles)

            # Rate limiting - be nice to the free API
            time.sleep(1)

            current_date = batch_end

        logger.info(f"✓ Total articles retrieved: {len(all_articles)}")
        return all_articles

    def save_to_json(self, data: list[dict], filename: str):
        """Save API data to JSON file"""
        output_path = self.output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Saved {len(data)} records to {output_path}")
        return output_path

    def get_coverage_info(self) -> dict:
        """
        Get information about what dates the API can cover

        Returns:
            Dictionary with coverage information
        """
        return {
            "api_type": "GDELT DOC/GEO API",
            "max_days_back": 90,
            "oldest_date": (datetime.now() - timedelta(days=90)).date(),
            "newest_date": datetime.now().date(),
            "rate_limit": "No official limit, but be respectful",
            "max_records_per_request": 250,
            "cost": "FREE - No API key required!",
            "authentication": "None required",
        }
