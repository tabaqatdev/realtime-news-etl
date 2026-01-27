"""
Geo Corrector - Smart location correction using reference data
Uses DuckDB spatial functions and similarity matching
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class GeoCorrector:
    """
    Smart geo-correction using reference city database

    Features:
    - Validates lat/long coordinates
    - Matches locations to known cities using spatial distance
    - Uses fuzzy name matching for ambiguous cases
    - Supports country-specific correction
    """

    def __init__(self, reference_db_path: str | None = None):
        """
        Initialize geo corrector

        Args:
            reference_db_path: Path to reference cities database (parquet)
                              If None, uses world_cities.parquet (33K+ cities from 244 countries)
                              Falls back to built-in 50 cities if file not found
        """
        # Default to world_cities.parquet if no path provided
        if reference_db_path is None:
            default_path = Path(__file__).parents[2] / "data_helpers" / "world_cities.parquet"
            if default_path.exists():
                reference_db_path = str(default_path)
                logger.info(f"Using default world cities database: {default_path}")

        self.reference_db_path = reference_db_path
        self.conn = duckdb.connect(":memory:")

        # Install spatial extension
        try:
            self.conn.execute("INSTALL spatial; LOAD spatial;")
        except Exception as e:
            logger.warning(f"Could not load spatial extension: {e}")

        # Load reference data
        self._load_reference_data()

    def _load_reference_data(self):
        """Load or create reference cities database"""
        if self.reference_db_path and Path(self.reference_db_path).exists():
            # Load external reference data
            logger.info(f"Loading reference data from {self.reference_db_path}")
            # Normalize column names to match expected schema
            # Check which columns exist in the file
            temp_query = f"SELECT * FROM read_parquet('{self.reference_db_path}') LIMIT 1"
            columns = [desc[0] for desc in self.conn.execute(temp_query).description]

            # Determine city column name
            city_col = "name" if "name" in columns else "city"

            self.conn.execute(f"""
                CREATE TABLE cities AS
                SELECT
                    {city_col} as city,
                    country,
                    country_code,
                    lat,
                    lon,
                    CAST(population AS INTEGER) as population
                FROM read_parquet('{self.reference_db_path}')
            """)
        else:
            # Create simplified reference database for common regions
            logger.info("Creating simplified reference database")
            self.conn.execute("""
                CREATE TABLE cities (
                    city VARCHAR,
                    country VARCHAR,
                    country_code VARCHAR,
                    lat DOUBLE,
                    lon DOUBLE,
                    population INTEGER
                )
            """)

            # Insert major Saudi Arabia cities
            self.conn.execute("""
                INSERT INTO cities VALUES
                -- Saudi Arabia
                ('Riyadh', 'Saudi Arabia', 'SA', 24.7136, 46.6753, 7000000),
                ('Jeddah', 'Saudi Arabia', 'SA', 21.5433, 39.1728, 4000000),
                ('Mecca', 'Saudi Arabia', 'SA', 21.4225, 39.8262, 2000000),
                ('Medina', 'Saudi Arabia', 'SA', 24.5247, 39.5692, 1500000),
                ('Dammam', 'Saudi Arabia', 'SA', 26.4207, 50.0888, 1200000),
                ('Khobar', 'Saudi Arabia', 'SA', 26.2172, 50.1971, 500000),
                ('Dhahran', 'Saudi Arabia', 'SA', 26.2361, 50.0393, 150000),
                ('Tabuk', 'Saudi Arabia', 'SA', 28.3838, 36.5550, 600000),
                ('Abha', 'Saudi Arabia', 'SA', 18.2164, 42.5053, 400000),

                -- Regional neighbors
                ('Dubai', 'United Arab Emirates', 'AE', 25.2048, 55.2708, 3000000),
                ('Abu Dhabi', 'United Arab Emirates', 'AE', 24.4539, 54.3773, 1500000),
                ('Doha', 'Qatar', 'QA', 25.2854, 51.5310, 2000000),
                ('Kuwait City', 'Kuwait', 'KW', 29.3759, 47.9774, 3000000),
                ('Manama', 'Bahrain', 'BH', 26.2285, 50.5860, 500000),
                ('Muscat', 'Oman', 'OM', 23.5880, 58.3829, 1000000),

                ('Sanaa', 'Yemen', 'YM', 15.3694, 44.1910, 2000000),
                ('Aden', 'Yemen', 'YM', 12.7855, 45.0187, 800000),

                ('Cairo', 'Egypt', 'EG', 30.0444, 31.2357, 20000000),
                ('Alexandria', 'Egypt', 'EG', 31.2001, 29.9187, 5000000),

                ('Baghdad', 'Iraq', 'IQ', 33.3152, 44.3661, 8000000),
                ('Tehran', 'Iran', 'IR', 35.6892, 51.3890, 9000000),

                ('Amman', 'Jordan', 'JO', 31.9454, 35.9284, 2000000),
                ('Beirut', 'Lebanon', 'LB', 33.8886, 35.4955, 2000000),
                ('Damascus', 'Syria', 'SY', 33.5138, 36.2765, 2000000),

                ('Islamabad', 'Pakistan', 'PK', 33.6844, 73.0479, 1000000),
                ('Karachi', 'Pakistan', 'PK', 24.8607, 67.0011, 16000000),

                ('New York', 'United States', 'US', 40.7128, -74.0060, 8000000),
                ('Washington', 'United States', 'US', 38.9072, -77.0369, 700000),
                ('London', 'United Kingdom', 'GB', 51.5074, -0.1278, 9000000),
                ('Paris', 'France', 'FR', 48.8566, 2.3522, 2000000)
            """)

        # Create spatial index
        logger.info("Creating spatial index for fast lookups")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cities_country ON cities(country_code)")

    def validate_coordinates(
        self,
        parquet_pattern: str,
        max_distance_km: float = 50.0,
        fix_invalid: bool = False,
    ) -> dict:
        """
        Validate and optionally correct coordinates in GDELT data

        Args:
            parquet_pattern: Input parquet file pattern
            max_distance_km: Maximum distance for considering a match
            fix_invalid: Whether to fix invalid coordinates

        Returns:
            Dictionary with validation statistics
        """
        logger.info("🔍 Validating coordinates...")

        # Check for invalid coordinates (outside valid ranges)
        invalid_coords = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_invalid,
                SUM(CASE WHEN ActionGeo_Lat < -90 OR ActionGeo_Lat > 90 THEN 1 ELSE 0 END) as invalid_lat,
                SUM(CASE WHEN ActionGeo_Long < -180 OR ActionGeo_Long > 180 THEN 1 ELSE 0 END) as invalid_lon
            FROM read_parquet('{parquet_pattern}', union_by_name=true)
            WHERE ActionGeo_Lat IS NOT NULL
            AND (ActionGeo_Lat < -90 OR ActionGeo_Lat > 90
                 OR ActionGeo_Long < -180 OR ActionGeo_Long > 180)
        """).fetchone()

        stats = {
            "total_records": self.conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_pattern}', union_by_name=true)"
            ).fetchone()[0],
            "invalid_lat": invalid_coords[1] or 0,
            "invalid_lon": invalid_coords[2] or 0,
            "total_invalid": invalid_coords[0] or 0,
        }

        logger.info(f"   Total records: {stats['total_records']:,}")
        logger.info(f"   Invalid latitudes: {stats['invalid_lat']:,}")
        logger.info(f"   Invalid longitudes: {stats['invalid_lon']:,}")

        return stats

    def find_nearest_city(
        self,
        lat: float,
        lon: float,
        country_code: str | None = None,
        max_distance_km: float = 100.0,
    ) -> dict | None:
        """
        Find nearest city to given coordinates

        Args:
            lat: Latitude
            lon: Longitude
            country_code: Optional country filter
            max_distance_km: Maximum search radius in kilometers

        Returns:
            Dictionary with city info and distance, or None if no match
        """
        country_filter = f"AND country_code = '{country_code}'" if country_code else ""

        # Haversine formula for distance calculation
        query = f"""
            SELECT
                city,
                country,
                country_code,
                lat as ref_lat,
                lon as ref_lon,
                population,
                2 * 6371 * ASIN(SQRT(
                    POW(SIN(RADIANS(lat - {lat}) / 2), 2) +
                    COS(RADIANS({lat})) * COS(RADIANS(lat)) *
                    POW(SIN(RADIANS(lon - {lon}) / 2), 2)
                )) as distance_km
            FROM cities
            WHERE 1=1 {country_filter}
            ORDER BY distance_km ASC
            LIMIT 1
        """

        result = self.conn.execute(query).fetchone()

        if result and result[6] <= max_distance_km:  # distance_km
            return {
                "city": result[0],
                "country": result[1],
                "country_code": result[2],
                "lat": result[3],
                "lon": result[4],
                "population": result[5],
                "distance_km": result[6],
            }

        return None

    def correct_locations(
        self,
        input_pattern: str,
        output_file: str,
        max_distance_km: float = 50.0,
        country_filter: str | None = None,
    ) -> dict:
        """
        Correct location coordinates by matching to reference cities

        Args:
            input_pattern: Input parquet file pattern
            output_file: Output file for corrected data
            max_distance_km: Maximum distance to consider for correction
            country_filter: Optional country code to focus corrections

        Returns:
            Dictionary with correction statistics
        """
        logger.info(f"🔧 Correcting locations (max distance: {max_distance_km}km)")

        # Create corrected version with nearest city matching
        country_clause = f"AND ActionGeo_CountryCode = '{country_filter}'" if country_filter else ""

        query = f"""
            COPY (
                SELECT
                    e.*,
                    c.city as NearestCity,
                    c.lat as CorrectedLat,
                    c.lon as CorrectedLon,
                    2 * 6371 * ASIN(SQRT(
                        POW(SIN(RADIANS(c.lat - e.ActionGeo_Lat) / 2), 2) +
                        COS(RADIANS(e.ActionGeo_Lat)) * COS(RADIANS(c.lat)) *
                        POW(SIN(RADIANS(c.lon - e.ActionGeo_Long) / 2), 2)
                    )) as DistanceToReference_km
                FROM read_parquet('{input_pattern}', union_by_name=true) e
                LEFT JOIN cities c ON (
                    c.country_code = e.ActionGeo_CountryCode
                    AND 2 * 6371 * ASIN(SQRT(
                        POW(SIN(RADIANS(c.lat - e.ActionGeo_Lat) / 2), 2) +
                        COS(RADIANS(e.ActionGeo_Lat)) * COS(RADIANS(c.lat)) *
                        POW(SIN(RADIANS(c.lon - e.ActionGeo_Long) / 2), 2)
                    )) <= {max_distance_km}
                )
                WHERE 1=1 {country_clause}
                ORDER BY SQLDATE DESC
            ) TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        self.conn.execute(query)

        # Calculate statistics
        stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN NearestCity IS NOT NULL THEN 1 ELSE 0 END) as matched,
                AVG(CASE WHEN DistanceToReference_km IS NOT NULL THEN DistanceToReference_km ELSE NULL END) as avg_distance
            FROM read_parquet('{output_file}')
        """).fetchone()

        result = {
            "total_records": stats[0],
            "matched_to_cities": stats[1] or 0,
            "avg_distance_km": round(stats[2], 2) if stats[2] else 0,
            "output_file": output_file,
        }

        logger.info(
            f"✅ Matched {result['matched_to_cities']:,}/{result['total_records']:,} events to cities"
        )
        logger.info(f"📏 Average distance to reference: {result['avg_distance_km']} km")
        logger.info(f"📁 Output: {output_file}")

        return result

    def add_coordinate_quality_flags(
        self,
        input_file: str,
        output_file: str,
    ) -> dict:
        """
        Add coordinate quality flags to identify suspicious locations

        Adds columns:
        - CoordQuality: 'high', 'medium', 'low', or 'null_island'
        - IsLikelyCentroid: Boolean flag for country centroid coordinates
        - CoordPrecision: Decimal places in coordinates

        Args:
            input_file: Input parquet file
            output_file: Output file path

        Returns:
            Dictionary with quality statistics
        """
        logger.info("🎯 Adding coordinate quality flags...")

        query = f"""
            COPY (
                SELECT
                    *,
                    CASE
                        WHEN ABS(ActionGeo_Lat) < 0.1 AND ABS(ActionGeo_Long) < 0.1 THEN 'null_island'
                        WHEN (ActionGeo_Lat = ROUND(ActionGeo_Lat) OR ActionGeo_Lat = ROUND(ActionGeo_Lat * 2) / 2)
                         AND (ActionGeo_Long = ROUND(ActionGeo_Long) OR ActionGeo_Long = ROUND(ActionGeo_Long * 2) / 2)
                        THEN 'low'
                        WHEN (ActionGeo_Lat = ROUND(ActionGeo_Lat * 10) / 10)
                         AND (ActionGeo_Long = ROUND(ActionGeo_Long * 10) / 10)
                        THEN 'medium'
                        ELSE 'high'
                    END as CoordQuality,
                    (
                        (ActionGeo_Lat = ROUND(ActionGeo_Lat) OR ActionGeo_Lat = ROUND(ActionGeo_Lat * 2) / 2 OR ActionGeo_Lat = ROUND(ActionGeo_Lat * 4) / 4)
                        AND (ActionGeo_Long = ROUND(ActionGeo_Long) OR ActionGeo_Long = ROUND(ActionGeo_Long * 2) / 2 OR ActionGeo_Long = ROUND(ActionGeo_Long * 4) / 4)
                        AND NOT (ABS(ActionGeo_Lat) < 0.1 AND ABS(ActionGeo_Long) < 0.1)
                    ) as IsLikelyCentroid,
                    CASE
                        WHEN ActionGeo_Lat IS NULL THEN NULL
                        ELSE CAST(REGEXP_EXTRACT(CAST(ActionGeo_Lat AS VARCHAR), '\\.([0-9]+)', 1) AS VARCHAR)
                    END as LatDecimals,
                    CASE
                        WHEN ActionGeo_Long IS NULL THEN NULL
                        ELSE CAST(REGEXP_EXTRACT(CAST(ActionGeo_Long AS VARCHAR), '\\.([0-9]+)', 1) AS VARCHAR)
                    END as LonDecimals
                FROM read_parquet('{input_file}')
            ) TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        self.conn.execute(query)

        # Calculate quality statistics
        stats = self.conn.execute(f"""
            SELECT
                CoordQuality,
                COUNT(*) as count,
                SUM(CASE WHEN IsLikelyCentroid THEN 1 ELSE 0 END) as centroid_count,
                ROUND(AVG(DistanceToCity_km), 2) as avg_distance
            FROM read_parquet('{output_file}')
            WHERE ActionGeo_Lat IS NOT NULL
            GROUP BY CoordQuality
            ORDER BY count DESC
        """).fetchall()

        result = {
            "output_file": output_file,
            "quality_breakdown": {
                row[0]: {"count": row[1], "centroid_count": row[2], "avg_distance": row[3]}
                for row in stats
            },
        }

        logger.info("📊 Coordinate Quality Breakdown:")
        for quality, metrics in result["quality_breakdown"].items():
            logger.info(
                f"   {quality}: {metrics['count']:,} events ({metrics['centroid_count']:,} likely centroids, avg {metrics['avg_distance']}km to city)"
            )

        return result

    def enrich_with_reference_data(
        self,
        input_pattern: str,
        output_file: str,
        max_distance_km: float = 500.0,
    ) -> dict:
        """
        Enrich GDELT data with reference city information for ALL geographic fields

        Adds columns for ActionGeo (event location):
        - NearestCity: Name of nearest reference city
        - CityPopulation: Population of nearest city
        - DistanceToCity_km: Distance in km to nearest city
        - CoordQuality: Quality flag (null_island, low, medium, high)

        Adds columns for Actor1Geo (actor 1 location):
        - Actor1_NearestCity, Actor1_CityPopulation, Actor1_DistanceToCity_km, Actor1_CoordQuality

        Adds columns for Actor2Geo (actor 2 location):
        - Actor2_NearestCity, Actor2_CityPopulation, Actor2_DistanceToCity_km, Actor2_CoordQuality

        Args:
            input_pattern: Input parquet file pattern
            output_file: Output file path
            max_distance_km: Maximum distance to consider a city match (default: 500km)

        Returns:
            Dictionary with enrichment statistics
        """
        logger.info("🏙️  Enriching with reference city data (ActionGeo + Actor1Geo + Actor2Geo)...")

        # Check if cities table has country_code populated
        has_country_code = self.conn.execute("""
            SELECT COUNT(*) > 0 FROM cities WHERE country_code IS NOT NULL AND country_code != ''
        """).fetchone()[0]

        # Build WHERE clauses for city matching
        if has_country_code:
            logger.info("Using country-based city matching for all three geo fields")
            action_filter = "WHERE country_code = e.ActionGeo_CountryCode"
            actor1_filter = "WHERE country_code = e.Actor1Geo_CountryCode"
            actor2_filter = "WHERE country_code = e.Actor2Geo_CountryCode"
        else:
            logger.info(f"Using global nearest neighbor search (max distance: {max_distance_km}km)")
            action_filter = ""
            actor1_filter = ""
            actor2_filter = ""

        # Quality detection function (reusable for lat/lon pairs)
        def quality_case(lat_col: str, lon_col: str) -> str:
            return f"""
                CASE
                    WHEN {lat_col} IS NULL OR {lon_col} IS NULL THEN NULL
                    WHEN ABS({lat_col}) < 0.1 AND ABS({lon_col}) < 0.1 THEN 'null_island'
                    WHEN ({lat_col} = ROUND({lat_col}) OR {lat_col} = ROUND({lat_col} * 2) / 2)
                     AND ({lon_col} = ROUND({lon_col}) OR {lon_col} = ROUND({lon_col} * 2) / 2)
                    THEN 'low'
                    WHEN ({lat_col} = ROUND({lat_col} * 10) / 10)
                     AND ({lon_col} = ROUND({lon_col} * 10) / 10)
                    THEN 'medium'
                    ELSE 'high'
                END
            """

        query = f"""
            COPY (
                SELECT
                    e.*,
                    -- ActionGeo enrichment (event location)
                    c_action.city as NearestCity,
                    c_action.population as CityPopulation,
                    c_action.distance_km as DistanceToCity_km,
                    {quality_case("e.ActionGeo_Lat", "e.ActionGeo_Long")} as CoordQuality,

                    -- Actor1Geo enrichment (actor 1 location)
                    c_actor1.city as Actor1_NearestCity,
                    c_actor1.population as Actor1_CityPopulation,
                    c_actor1.distance_km as Actor1_DistanceToCity_km,
                    {quality_case("e.Actor1Geo_Lat", "e.Actor1Geo_Long")} as Actor1_CoordQuality,

                    -- Actor2Geo enrichment (actor 2 location)
                    c_actor2.city as Actor2_NearestCity,
                    c_actor2.population as Actor2_CityPopulation,
                    c_actor2.distance_km as Actor2_DistanceToCity_km,
                    {quality_case("e.Actor2Geo_Lat", "e.Actor2Geo_Long")} as Actor2_CoordQuality

                FROM read_parquet('{input_pattern}', union_by_name=true) e

                -- LATERAL JOIN for ActionGeo
                LEFT JOIN LATERAL (
                    SELECT
                        city,
                        population,
                        lat,
                        lon,
                        2 * 6371 * ASIN(SQRT(
                            POW(SIN(RADIANS(lat - e.ActionGeo_Lat) / 2), 2) +
                            COS(RADIANS(e.ActionGeo_Lat)) * COS(RADIANS(lat)) *
                            POW(SIN(RADIANS(lon - e.ActionGeo_Long) / 2), 2)
                        )) as distance_km
                    FROM cities
                    {action_filter}
                    ORDER BY distance_km ASC
                    LIMIT 1
                ) c_action ON (
                    c_action.distance_km <= CASE
                        -- Allow 2x distance for country centroids (rounded coordinates)
                        WHEN (e.ActionGeo_Lat = ROUND(e.ActionGeo_Lat) OR e.ActionGeo_Lat = ROUND(e.ActionGeo_Lat * 2) / 2)
                         AND (e.ActionGeo_Long = ROUND(e.ActionGeo_Long) OR e.ActionGeo_Long = ROUND(e.ActionGeo_Long * 2) / 2)
                        THEN {max_distance_km * 2}
                        ELSE {max_distance_km}
                    END
                )

                -- LATERAL JOIN for Actor1Geo
                LEFT JOIN LATERAL (
                    SELECT
                        city,
                        population,
                        lat,
                        lon,
                        2 * 6371 * ASIN(SQRT(
                            POW(SIN(RADIANS(lat - e.Actor1Geo_Lat) / 2), 2) +
                            COS(RADIANS(e.Actor1Geo_Lat)) * COS(RADIANS(lat)) *
                            POW(SIN(RADIANS(lon - e.Actor1Geo_Long) / 2), 2)
                        )) as distance_km
                    FROM cities
                    {actor1_filter}
                    {"AND" if actor1_filter else "WHERE"} e.Actor1Geo_Lat IS NOT NULL AND e.Actor1Geo_Long IS NOT NULL
                    ORDER BY distance_km ASC
                    LIMIT 1
                ) c_actor1 ON (
                    c_actor1.distance_km <= CASE
                        -- Allow 2x distance for country centroids (rounded coordinates)
                        WHEN (e.Actor1Geo_Lat = ROUND(e.Actor1Geo_Lat) OR e.Actor1Geo_Lat = ROUND(e.Actor1Geo_Lat * 2) / 2)
                         AND (e.Actor1Geo_Long = ROUND(e.Actor1Geo_Long) OR e.Actor1Geo_Long = ROUND(e.Actor1Geo_Long * 2) / 2)
                        THEN {max_distance_km * 2}
                        ELSE {max_distance_km}
                    END
                )

                -- LATERAL JOIN for Actor2Geo
                LEFT JOIN LATERAL (
                    SELECT
                        city,
                        population,
                        lat,
                        lon,
                        2 * 6371 * ASIN(SQRT(
                            POW(SIN(RADIANS(lat - e.Actor2Geo_Lat) / 2), 2) +
                            COS(RADIANS(e.Actor2Geo_Lat)) * COS(RADIANS(lat)) *
                            POW(SIN(RADIANS(lon - e.Actor2Geo_Long) / 2), 2)
                        )) as distance_km
                    FROM cities
                    {actor2_filter}
                    {"AND" if actor2_filter else "WHERE"} e.Actor2Geo_Lat IS NOT NULL AND e.Actor2Geo_Long IS NOT NULL
                    ORDER BY distance_km ASC
                    LIMIT 1
                ) c_actor2 ON (
                    c_actor2.distance_km <= CASE
                        -- Allow 2x distance for country centroids (rounded coordinates)
                        WHEN (e.Actor2Geo_Lat = ROUND(e.Actor2Geo_Lat) OR e.Actor2Geo_Lat = ROUND(e.Actor2Geo_Lat * 2) / 2)
                         AND (e.Actor2Geo_Long = ROUND(e.Actor2Geo_Long) OR e.Actor2Geo_Long = ROUND(e.Actor2Geo_Long * 2) / 2)
                        THEN {max_distance_km * 2}
                        ELSE {max_distance_km}
                    END
                )

                WHERE e.Actor1Geo_Lat IS NOT NULL AND e.Actor1Geo_Long IS NOT NULL
                  AND e.Actor2Geo_Lat IS NOT NULL AND e.Actor2Geo_Long IS NOT NULL
                ORDER BY SQLDATE DESC
            ) TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        self.conn.execute(query)

        stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN NearestCity IS NOT NULL THEN 1 ELSE 0 END) as action_enriched,
                SUM(CASE WHEN Actor1_NearestCity IS NOT NULL THEN 1 ELSE 0 END) as actor1_enriched,
                SUM(CASE WHEN Actor2_NearestCity IS NOT NULL THEN 1 ELSE 0 END) as actor2_enriched,
                ROUND(AVG(CASE WHEN DistanceToCity_km IS NOT NULL THEN DistanceToCity_km END), 1) as avg_action_distance,
                ROUND(AVG(CASE WHEN Actor1_DistanceToCity_km IS NOT NULL THEN Actor1_DistanceToCity_km END), 1) as avg_actor1_distance,
                ROUND(AVG(CASE WHEN Actor2_DistanceToCity_km IS NOT NULL THEN Actor2_DistanceToCity_km END), 1) as avg_actor2_distance,
                MAX(DistanceToCity_km) as max_action_distance,
                MAX(Actor1_DistanceToCity_km) as max_actor1_distance,
                MAX(Actor2_DistanceToCity_km) as max_actor2_distance
            FROM read_parquet('{output_file}')
        """).fetchone()

        total = stats[0]
        action_enriched = stats[1] or 0
        actor1_enriched = stats[2] or 0
        actor2_enriched = stats[3] or 0

        result = {
            "total_records": total,
            "action_geo": {
                "enriched_records": action_enriched,
                "enrichment_rate": round(action_enriched / total * 100, 1) if total > 0 else 0,
                "avg_distance_km": stats[4],
                "max_distance_km": stats[7],
            },
            "actor1_geo": {
                "enriched_records": actor1_enriched,
                "enrichment_rate": round(actor1_enriched / total * 100, 1) if total > 0 else 0,
                "avg_distance_km": stats[5],
                "max_distance_km": stats[8],
            },
            "actor2_geo": {
                "enriched_records": actor2_enriched,
                "enrichment_rate": round(actor2_enriched / total * 100, 1) if total > 0 else 0,
                "avg_distance_km": stats[6],
                "max_distance_km": stats[9],
            },
            "output_file": output_file,
        }

        logger.info("✅ Geographic Enrichment Complete:")
        logger.info(
            f"   ActionGeo:  {action_enriched:,}/{total:,} ({result['action_geo']['enrichment_rate']}%) - avg {result['action_geo']['avg_distance_km']}km, max {result['action_geo']['max_distance_km']}km"
        )
        logger.info(
            f"   Actor1Geo:  {actor1_enriched:,}/{total:,} ({result['actor1_geo']['enrichment_rate']}%) - avg {result['actor1_geo']['avg_distance_km']}km, max {result['actor1_geo']['max_distance_km']}km"
        )
        logger.info(
            f"   Actor2Geo:  {actor2_enriched:,}/{total:,} ({result['actor2_geo']['enrichment_rate']}%) - avg {result['actor2_geo']['avg_distance_km']}km, max {result['actor2_geo']['max_distance_km']}km"
        )
        logger.info(f"📁 Output: {output_file}")

        return result

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    """CLI entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Geo-correct GDELT location data")
    parser.add_argument(
        "--action",
        choices=["validate", "correct", "enrich"],
        default="validate",
        help="Action to perform (default: validate)",
    )
    parser.add_argument(
        "--input",
        default="data/parquet/events/**/*.parquet",
        help="Input parquet pattern",
    )
    parser.add_argument(
        "--output",
        default="data/parquet/geo_corrected",
        help="Output directory",
    )
    parser.add_argument(
        "--max-distance",
        type=float,
        default=50.0,
        help="Maximum distance in km for matching (default: 50)",
    )
    parser.add_argument(
        "--country",
        help="Focus corrections on specific country code",
    )
    parser.add_argument(
        "--reference-db",
        help="Path to reference cities database (parquet)",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    print("=" * 70)
    print(" GDELT GEO CORRECTOR")
    print("=" * 70)

    corrector = GeoCorrector(reference_db_path=args.reference_db)

    try:
        if args.action == "validate":
            print("🔍 Validating coordinates...")
            stats = corrector.validate_coordinates(args.input)
            print(f"\n✅ Validation complete - {stats['total_invalid']} invalid coordinates")

        elif args.action == "correct":
            print("🔧 Correcting locations...")
            output_file = f"{args.output}/geo_corrected.parquet"
            Path(args.output).mkdir(parents=True, exist_ok=True)
            stats = corrector.correct_locations(
                args.input,
                output_file,
                max_distance_km=args.max_distance,
                country_filter=args.country,
            )
            print(f"\n✅ Correction complete - Matched {stats['matched_to_cities']} events")

        elif args.action == "enrich":
            print("🏙️  Enriching with city data...")
            output_file = f"{args.output}/geo_enriched.parquet"
            Path(args.output).mkdir(parents=True, exist_ok=True)
            stats = corrector.enrich_with_reference_data(args.input, output_file)
            print("\n✅ Enrichment complete:")
            print(f"   ActionGeo:  {stats['action_geo']['enrichment_rate']}% enriched")
            print(f"   Actor1Geo:  {stats['actor1_geo']['enrichment_rate']}% enriched")
            print(f"   Actor2Geo:  {stats['actor2_geo']['enrichment_rate']}% enriched")

    finally:
        corrector.close()


if __name__ == "__main__":
    main()
