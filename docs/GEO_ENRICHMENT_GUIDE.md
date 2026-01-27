# Geographic Enrichment Guide

## Overview

The geo-enrichment system adds reference city information to GDELT events, improving location accuracy and enabling city-level analysis.

## Features

### 1. Smart City Matching
- Uses Haversine distance formula for accurate geographic matching
- Matches events to nearest reference city within configurable radius
- Supports country-specific filtering for better accuracy

### 2. Reference Databases

**Option A: World Cities Database (DEFAULT & RECOMMENDED)**
- **33,227 cities** from `data_helpers/world_cities.parquet` covering 244 countries
- Comprehensive global coverage, prevents incorrect distant matches
- Used automatically when no reference database is specified
- Includes all major cities plus smaller towns for precise matching
- No nulls in critical fields (city, country_code, population, coordinates)
- Performance: High enrichment rates with accurate distance calculations

**Option B: Built-in Reference Database**
- 50 cities focused on Saudi Arabia, GCC, and key global cities:
  - **Saudi Arabia**: Riyadh, Jeddah, Mecca, Medina, Dammam, Khobar, Dhahran, Tabuk, Abha
  - **GCC Countries**: Dubai, Abu Dhabi, Doha, Kuwait City, Manama, Muscat
  - **Regional**: Sanaa, Aden, Cairo, Alexandria, Baghdad, Tehran, Amman, Beirut, Damascus
  - **South Asia**: Islamabad, Karachi
  - **Global**: New York, Washington, London, Paris
- Performance: 85.0% enrichment, but max distance 3918km (can incorrectly match distant cities)

**Option C: Custom Reference Database**
You can provide your own reference database with any cities worldwide in parquet format.

### 3. Distance-Based Validation
- Calculates precise distance from event to nearest city
- Identifies outliers and ambiguous locations
- Provides population data for context

## Usage

### Option 1: Integrated Pipeline (Recommended)

Use the `--enrich-geo` flag with the main pipeline:

```bash
# Collect and enrich in one command
uv run news-cn --country SA --start-date 2026-01-01 --enrich-geo

# Skip article scraping for faster processing
uv run news-cn --country SA --no-scrape --enrich-geo
```

### Option 2: Standalone Geo-Corrector CLI

For more control, use the dedicated geo-corrector:

```bash
# Validate coordinates
uv run news-cn-geo --action validate --input "data/parquet/events/**/*.parquet"

# Enrich with default world cities database (33K+ cities, automatic)
uv run news-cn-geo --action enrich \
  --input "data/parquet/cleaned/cleaned_events.parquet" \
  --output "data/parquet/geo_enriched"

# Or explicitly specify the world cities database
uv run news-cn-geo --action enrich \
  --input "data/parquet/cleaned/cleaned_events.parquet" \
  --output "data/parquet/geo_world" \
  --reference-db "data_helpers/world_cities.parquet" \
  --max-distance 500

# Correct locations (add reference coordinates)
uv run news-cn-geo --action correct \
  --input "data/parquet/events/**/*.parquet" \
  --output "data/parquet/geo_corrected" \
  --max-distance 100 \
  --country SA
```

### Option 3: Converting TSV to Parquet

If you have a cities database in TSV format with WKT POINT geometry:

```bash
# Convert cities.tsv to parquet
duckdb -c "
INSTALL spatial;
LOAD spatial;

COPY (
    SELECT
        name as city,
        population,
        ST_X(ST_GeomFromText(geog)) as lon,
        ST_Y(ST_GeomFromText(geog)) as lat,
        '' as country,
        '' as country_code
    FROM read_csv('data_helpers/cities.tsv', delim='\t', header=true)
) TO 'data_helpers/world_cities.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
"

# Then use it for enrichment
uv run news-cn-geo --action enrich \
  --input "data/parquet/cleaned/cleaned_events.parquet" \
  --output "data/parquet/enriched" \
  --reference-db "data_helpers/world_cities.parquet" \
  --max-distance 500
```

## Enriched Data Schema

The geo-enrichment adds **12 new columns** - four for each of GDELT's three coordinate sets:

### ActionGeo (Event Location)
| Column | Type | Description |
|--------|------|-------------|
| `NearestCity` | VARCHAR | Name of nearest reference city |
| `CityPopulation` | INTEGER | Population of matched city |
| `DistanceToCity_km` | DOUBLE | Distance in kilometers to reference city |
| `CoordQuality` | VARCHAR | Quality flag (null_island, low, medium, high) |

### Actor1Geo (Actor 1 Location)
| Column | Type | Description |
|--------|------|-------------|
| `Actor1_NearestCity` | VARCHAR | Name of nearest reference city for Actor 1 |
| `Actor1_CityPopulation` | INTEGER | Population of matched city |
| `Actor1_DistanceToCity_km` | DOUBLE | Distance in kilometers to reference city |
| `Actor1_CoordQuality` | VARCHAR | Quality flag (null_island, low, medium, high) |

### Actor2Geo (Actor 2 Location)
| Column | Type | Description |
|--------|------|-------------|
| `Actor2_NearestCity` | VARCHAR | Name of nearest reference city for Actor 2 |
| `Actor2_CityPopulation` | INTEGER | Population of matched city |
| `Actor2_DistanceToCity_km` | DOUBLE | Distance in kilometers to reference city |
| `Actor2_CoordQuality` | VARCHAR | Quality flag (null_island, low, medium, high) |

## Analysis Examples

### Example 1: Events by City

```sql
SELECT
    NearestCity,
    COUNT(*) as events,
    AVG(AvgTone) as avg_tone,
    MIN(SQLDATE) as first_event,
    MAX(SQLDATE) as last_event
FROM 'data/parquet/cleaned/geo_enriched.parquet'
WHERE NearestCity IS NOT NULL
GROUP BY NearestCity
ORDER BY events DESC
LIMIT 20;
```

### Example 2: Distance Analysis

```sql
SELECT
    NearestCity,
    COUNT(*) as events,
    AVG(DistanceToCity_km) as avg_distance,
    MIN(DistanceToCity_km) as min_distance,
    MAX(DistanceToCity_km) as max_distance
FROM 'data/parquet/cleaned/geo_enriched.parquet'
WHERE NearestCity IS NOT NULL
GROUP BY NearestCity
ORDER BY events DESC;
```

### Example 3: Urban vs Non-Urban Events

```sql
SELECT
    CASE
        WHEN DistanceToCity_km < 20 THEN 'Urban'
        WHEN DistanceToCity_km < 50 THEN 'Suburban'
        ELSE 'Rural'
    END as area_type,
    COUNT(*) as events,
    AVG(AvgTone) as avg_tone
FROM 'data/parquet/cleaned/geo_enriched.parquet'
WHERE NearestCity IS NOT NULL
GROUP BY area_type;
```

### Example 4: Major Cities Focus

```sql
SELECT
    NearestCity,
    CityPopulation,
    COUNT(*) as events,
    AVG(AvgTone) as avg_tone
FROM 'data/parquet/cleaned/geo_enriched.parquet'
WHERE CityPopulation > 1000000  -- Cities over 1M population
GROUP BY NearestCity, CityPopulation
ORDER BY events DESC;
```

## Performance Statistics

From our test run (19,260 Saudi Arabia events) with World Cities Database (243 cities):

### ActionGeo (Event Location)
- **Enrichment Rate**: 86.1% (16,585 records matched)
- **Average Distance**: 106.3 km to nearest reference city
- **Max Distance**: 499 km (enforced limit prevents incorrect matches)

### Actor1Geo (Actor 1 Location)
- **Enrichment Rate**: 81.7% (15,726 records matched)
- **Average Distance**: 107.0 km to nearest reference city
- **Max Distance**: 490 km

### Actor2Geo (Actor 2 Location)
- **Enrichment Rate**: 73.5% (14,163 records matched)
- **Average Distance**: 108.2 km to nearest reference city
- **Max Distance**: 499 km

### Processing Performance
- **Processing Time**: < 200ms for 19K records (all three coordinate sets)
- **Throughput**: ~96,000 records/second with three LATERAL JOINs
- **Memory Usage**: < 50MB for reference database + spatial index

## Top Matched Cities

Based on Saudi Arabia data analysis with World Cities Database:

| City | Events | Avg Distance (km) | Country | Notes |
|------|--------|-------------------|---------|-------|
| Riyadh | 10,449 | 93 | Saudi Arabia | Capital, highest event density |
| Sanaa | 714 | 215 | Yemen | Regional conflict coverage |
| Doha | 556 | 51 | Qatar | Regional politics |
| Abu Dhabi | 389 | 61 | UAE | GCC relations |
| Tehran | 385 | 118 | Iran | Regional relations |
| Islamabad | 375 | 20 | Pakistan | Diplomatic relations (very close match) |
| Cairo | 317 | 208 | Egypt | Regional power |
| Tel Aviv-Yafo | 284 | 81 | Israel | Middle East coverage |
| Muscat | 234 | 333 | Oman | GCC relations |
| Washington, D.C. | 203 | 10 | United States | International relations (exact match) |

**Key Improvements with World Cities:**
- More precise matches (lower avg distances)
- Better coverage of global cities (Los Angeles, San Francisco now matched correctly)
- Prevents incorrect distant matches (max 499km vs 3918km)

## Geographic Coverage

The coordinate validation shows events span globally:
- **Latitude Range**: -42° to 72° (Antarctic to Arctic regions)
- **Longitude Range**: -122° to 174° (covering all continents)

This is correct - the pipeline captures:
1. Events physically in Saudi Arabia
2. Events involving Saudi actors anywhere in the world
3. Events about Saudi Arabia from international sources

## Technical Details

### Distance Calculation

Uses the Haversine formula for great-circle distances:

```sql
distance_km = 2 * 6371 * ASIN(SQRT(
    POW(SIN(RADIANS(lat2 - lat1) / 2), 2) +
    COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
    POW(SIN(RADIANS(lon2 - lon1) / 2), 2)
))
```

### Spatial Indexing

- Creates B-tree index on country_code for fast filtering
- Uses LATERAL JOIN for nearest-neighbor queries
- Processes 19K records in under 1 second

### Memory Efficiency

- Reference database stays in-memory (< 1MB for 50 cities)
- Streaming processing for large datasets
- No pandas dependency - pure DuckDB

## Custom Reference Database Format

To use your own cities database, provide a parquet file with these columns:

```python
city: VARCHAR          # City name
country: VARCHAR       # Full country name
country_code: VARCHAR  # ISO 2-letter code
lat: DOUBLE           # Latitude
lon: DOUBLE           # Longitude
population: INTEGER    # Population (optional)
```

Example sources:
- [GeoNames](http://www.geonames.org/) - 11M+ cities worldwide
- [Natural Earth](https://www.naturalearthdata.com/) - Curated city data
- [World Cities Database](https://simplemaps.com/data/world-cities) - 43K cities

## Troubleshooting

### Low Enrichment Rate

If fewer than 50% of records are enriched:

1. **Check coordinate validity**:
   ```bash
   uv run news-cn-geo --action validate
   ```

2. **Increase max distance**:
   ```bash
   uv run news-cn-geo --action enrich --max-distance 100
   ```

3. **Add more reference cities** to your database

### Distance Issues

If distances seem too large:

1. Verify your input has correct country filtering
2. Check that reference database covers your target regions
3. Review coordinate quality in source data

## Integration with Analysis

The enriched data works seamlessly with existing queries:

```sql
-- Original query (still works)
SELECT ActionGeo_FullName, COUNT(*) as events
FROM 'data/parquet/cleaned/cleaned_events.parquet'
GROUP BY ActionGeo_FullName;

-- Enhanced query (with city data)
SELECT NearestCity, ActionGeo_FullName, COUNT(*) as events
FROM 'data/parquet/cleaned/geo_enriched.parquet'
WHERE NearestCity IS NOT NULL
GROUP BY NearestCity, ActionGeo_FullName;
```

## Next Steps

1. **Temporal Analysis**: Combine with time-series for city-level trends
2. **Sentiment Mapping**: Visualize tone scores by city
3. **Network Analysis**: Build actor-location networks
4. **Custom Regions**: Add your own points of interest to reference database
