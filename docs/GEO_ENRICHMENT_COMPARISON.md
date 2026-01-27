# Geographic Enrichment Quality Comparison

## Problem Statement

The initial geo-enrichment implementation using a built-in 50-city reference database had quality issues:

1. **Extreme distances**: Events in San Francisco (37.77°N, 122.42°W) were matched to Washington DC (38.91°N, 77.04°W) - a distance of **3,918 km**
2. **Limited coverage**: Only 85% of events matched to cities
3. **Incorrect matches**: Events "in the middle of the ocean" or remote areas were matched to very distant cities

## Solution

Converted the comprehensive `data_helpers/cities.tsv` (243 world cities) to parquet format and implemented:

1. **Global nearest-neighbor search** instead of country-filtered matching
2. **Maximum distance limit** (500km) to prevent incorrect distant matches
3. **Automatic detection** of whether reference database has country codes

## Results Comparison

| Metric | Built-in (50 cities) | World Cities (243) | Improvement |
|--------|---------------------|-------------------|-------------|
| **Total Records** | 19,260 | 19,260 | - |
| **Enriched** | 16,378 (85.0%) | 16,585 (86.1%) | +1.1% |
| **Avg Distance** | 139 km | 106 km | **-24%** |
| **Max Distance** | 3,918 km ❌ | 499 km ✅ | **-87%** |
| **Processing Time** | < 1 second | < 1 second | Same |

## Example Fixes

### San Francisco Events (Before)
```
San Francisco (37.77, -122.42) → Washington DC
Distance: 3,918 km ❌ INCORRECT
```

### San Francisco Events (After)
```
Hollywood, CA (34.10, -118.33) → Los Angeles
Distance: 17.8 km ✅

Napa Valley, CA (38.26, -122.29) → San Francisco
Distance: 55.7 km ✅
```

## Top Cities - Quality Comparison

### Built-in Database
```
1. Riyadh - 10,466 events
2. Jeddah - 773 events
3. Sanaa - 693 events
...
10. Washington - 360 events (includes WRONG San Francisco matches!)
```

### World Cities Database
```
1. Riyadh - 10,449 events (avg 93km) ✅
2. Sanaa - 714 events (avg 215km) ✅
3. Doha - 556 events (avg 51km) ✅
4. Abu Dhabi - 389 events (avg 61km) ✅
5. Tehran - 385 events (avg 118km) ✅
6. Islamabad - 375 events (avg 20km) ✅ VERY PRECISE
7. Cairo - 317 events (avg 208km) ✅
8. Tel Aviv-Yafo - 284 events (avg 81km) ✅
...
NEW: Los Angeles - matches Hollywood correctly (17.8km) ✅
NEW: San Francisco - matches Napa Valley correctly (55.7km) ✅
```

## Code Changes

### geo_corrector.py Enhancement

**Before:**
```python
# Always filtered by country code
WHERE country_code = e.ActionGeo_CountryCode
```

**After:**
```python
# Auto-detect if country codes are available
has_country_code = self.conn.execute("""
    SELECT COUNT(*) > 0 FROM cities
    WHERE country_code IS NOT NULL AND country_code != ''
""").fetchone()[0]

if has_country_code:
    city_filter = "WHERE country_code = e.ActionGeo_CountryCode"
else:
    city_filter = ""  # Global search with distance limit

# Distance limit prevents incorrect matches
ON c.distance_km <= {max_distance_km}  # 500km default
```

## Usage

### Recommended Command
```bash
# Convert TSV to Parquet (one-time)
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

# Enrich with world cities
uv run news-cn-geo --action enrich \
  --input "data/parquet/cleaned/cleaned_events.parquet" \
  --output "data/parquet/geo_world" \
  --reference-db "data_helpers/world_cities.parquet" \
  --max-distance 500
```

## Quality Validation

### Check for "Middle of Ocean" Events
```sql
-- Events with no city match (NULL)
SELECT
    ActionGeo_Lat,
    ActionGeo_Long,
    ActionGeo_FullName,
    ActionGeo_CountryCode
FROM 'data/parquet/geo_world/geo_enriched.parquet'
WHERE NearestCity IS NULL
ORDER BY ActionGeo_Lat, ActionGeo_Long
LIMIT 20;

-- Result: Argentina, Australia, Brazil, Indonesia
-- These are CORRECT - no nearby cities within 500km
```

### Distance Distribution
```sql
SELECT
    COUNT(*) as total_enriched,
    ROUND(AVG(DistanceToCity_km), 1) as avg_km,
    ROUND(MIN(DistanceToCity_km), 1) as min_km,
    ROUND(MAX(DistanceToCity_km), 1) as max_km,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DistanceToCity_km), 1) as median_km,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY DistanceToCity_km), 1) as p75_km,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DistanceToCity_km), 1) as p95_km
FROM 'data/parquet/geo_world/geo_enriched.parquet'
WHERE NearestCity IS NOT NULL;

-- Result:
-- avg: 106km, median: 56km, p95: 383km, max: 499km ✅
```

## Conclusion

The comprehensive world cities database provides:

1. **Better accuracy**: Max distance reduced from 3,918km to 499km (-87%)
2. **Better coverage**: +207 more events enriched (+1.1%)
3. **Better precision**: Average distance reduced from 139km to 106km (-24%)
4. **Same performance**: Still < 1 second for 19K records

**Status:** ✅ Geographic enrichment quality issues RESOLVED
