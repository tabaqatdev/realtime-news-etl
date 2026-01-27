"""
GDELT GKG GeoJSON API Client
Alternative API that uses the stable GKG GeoJSON endpoint
100% FREE - No API keys required!

Based on: https://blog.gdeltproject.org/announcing-our-first-api-gkg-geojson/
"""

import json
import logging
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GDELTGKGAPIClient:
    """
    Client for GDELT's GKG GeoJSON API
    More stable alternative to the DOC API
    No authentication required!
    """

    def __init__(self, output_dir: str = "data/api"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # GKG GeoJSON API endpoint (more stable)
        self.gkg_api = "https://api.gdeltproject.org/api/v1/gkg_geojson"

    def query_saudi_news(
        self, timespan_minutes: int = 60, output_type: int = 1, max_rows: int = 5000
    ) -> dict | None:
        """
        Query GDELT GKG API for Saudi Arabia news

        API Docs: https://blog.gdeltproject.org/announcing-our-first-api-gkg-geojson/

        Args:
            timespan_minutes: How far back to search (15-1440 minutes, default 60)
            output_type: 1=article level, 2=location+time collapsed
            max_rows: Maximum records to return (1-250000, default 5000)

        Returns:
            GeoJSON dictionary or None if failed
        """
        # Clamp values to API limits
        timespan_minutes = max(15, min(1440, timespan_minutes))
        max_rows = max(1, min(250000, max_rows))

        logger.info(f"📡 Querying GKG API for Saudi Arabia (last {timespan_minutes} minutes)")

        params = {
            "QUERY": "geoname:Saudi Arabia",  # Filter to Saudi Arabia locations
            "TIMESPAN": timespan_minutes,
            "OUTPUTTYPE": output_type,
            "OUTPUTFIELDS": "url,name,domain,lang,tone,sharingimage",
            "MAXROWS": max_rows,
        }

        try:
            response = requests.get(self.gkg_api, params=params, timeout=60)
            response.raise_for_status()

            # Check if response is empty
            if not response.text or len(response.text.strip()) == 0:
                logger.warning("⚠ Empty response from API")
                return None

            data = response.json()

            if "features" in data:
                feature_count = len(data["features"])
                logger.info(f"✓ Retrieved {feature_count} locations from Saudi Arabia")
                return data
            else:
                logger.warning("⚠ No 'features' key in GeoJSON")
                return None

        except requests.Timeout as e:
            logger.error(f"✗ API request timed out: {e}")
            logger.info("💡 Tip: Try again or use a shorter timespan")
            return None
        except requests.RequestException as e:
            logger.error(f"✗ API request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"✗ Failed to parse JSON response: {e}")
            logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
            return None

    def get_saudi_news_last_hour(self) -> dict | None:
        """Quick method to get last hour of Saudi news"""
        return self.query_saudi_news(timespan_minutes=60, max_rows=5000)

    def get_saudi_news_last_24h(self) -> dict | None:
        """Get last 24 hours of Saudi news (may be large!)"""
        return self.query_saudi_news(timespan_minutes=1440, max_rows=10000)

    def get_saudi_news_by_theme(self, theme: str, timespan_minutes: int = 60) -> dict | None:
        """
        Get Saudi news filtered by GDELT theme

        Common themes:
        - FOOD_SECURITY
        - TERROR
        - HEALTH
        - EDUCATION
        - CLIMATE_CHANGE
        - etc.

        Args:
            theme: GDELT theme code
            timespan_minutes: How far back to search

        Returns:
            GeoJSON dictionary or None
        """
        logger.info(f"📡 Querying GKG API for '{theme}' in Saudi Arabia")

        params = {
            "QUERY": f"geoname:Saudi Arabia,{theme}",
            "TIMESPAN": timespan_minutes,
            "OUTPUTTYPE": 1,
            "OUTPUTFIELDS": "url,name,domain,lang,tone,themes",
            "MAXROWS": 5000,
        }

        try:
            response = requests.get(self.gkg_api, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            feature_count = len(data.get("features", []))
            logger.info(f"✓ Retrieved {feature_count} locations")
            return data

        except Exception as e:
            logger.error(f"✗ Query failed: {e}")
            return None

    def save_to_geojson(self, data: dict, filename: str) -> Path:
        """Save GeoJSON data to file"""
        output_path = self.output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        feature_count = len(data.get("features", []))
        logger.info(f"💾 Saved {feature_count} features to {output_path}")
        return output_path

    def convert_geojson_to_articles(self, geojson_data: dict) -> list[dict]:
        """
        Convert GeoJSON features to a simpler article list format

        Args:
            geojson_data: GeoJSON dictionary from API

        Returns:
            List of article dictionaries
        """
        articles = []

        for feature in geojson_data.get("features", []):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])

            article = {
                "url": props.get("url", ""),
                "title": props.get("name", "No title"),
                "location": props.get("name", ""),
                "domain": props.get("domain", ""),
                "language": props.get("urllangcode", ""),
                "tone": props.get("urltone", 0),
                "social_image": props.get("urlsocialimage", ""),
                "timestamp": props.get("urlpubtimedate", ""),
                "latitude": coords[1] if len(coords) > 1 else None,
                "longitude": coords[0] if len(coords) > 0 else None,
            }
            articles.append(article)

        return articles

    def get_coverage_info(self) -> dict:
        """Get information about API capabilities"""
        return {
            "api_type": "GDELT GKG GeoJSON API",
            "max_timespan": "24 hours (1440 minutes)",
            "min_timespan": "15 minutes",
            "update_frequency": "Every 15 minutes",
            "geographic_filter": "Supports country and ADM1 filtering",
            "theme_search": "Supports GDELT theme filtering",
            "max_records": "250,000 per request",
            "cost": "FREE - No API key required!",
            "authentication": "None required",
            "documentation": "https://blog.gdeltproject.org/announcing-our-first-api-gkg-geojson/",
        }
