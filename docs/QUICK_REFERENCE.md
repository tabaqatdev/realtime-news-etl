# Quick Reference Card

## 🚀 Common Commands

```bash
# === PIPELINE OPERATIONS ===
uv run news-cn                    # Collect GDELT data (incremental)
uv run news-cn-tools stats        # View pipeline statistics
uv run news-cn-tools consolidate  # Manually consolidate days
uv run news-cn-diagnose          # Run system diagnostics

# === BATCH PROCESSING (10x Faster) ===
# For bulk historical data - see examples below

# === DATA CLEANING ===
uv run news-cn-clean              # Clean & normalize country codes
                                  # Output: data/cleaned/saudi_events_cleaned.parquet

# === ARTICLE SCRAPING ===
# Basic (Jina AI - free but rate limited)
uv run news-cn-scrape 10

# Advanced (nodriver - free, no limits, bypasses anti-bot)
uv add nodriver                   # First time only
uv run news-cn-scrape-advanced 20

# With Firecrawl fallback (requires API key)
export FIRECRAWL_API_KEY="fc-..."
uv add nodriver firecrawl-py      # First time only
uv run news-cn-scrape-advanced 20

# === DATA QUERYING ===
# Count total events
uv run duckdb -c "SELECT COUNT(*) FROM 'data/parquet/events/**/*.parquet'"

# Query cleaned data
uv run duckdb -c "SELECT * FROM 'data/cleaned/saudi_events_cleaned.parquet' LIMIT 10"

# Recent events with URLs
uv run duckdb -c "
  SELECT SQLDATE, Actor1Name, Actor2Name, SOURCEURL
  FROM 'data/parquet/events/**/*.parquet'
  ORDER BY SQLDATE DESC LIMIT 10
"

# === EXAMPLES ===
uv run examples/complete_pipeline_demo.py
uv run examples/article_enrichment_example.py

# === BATCH PROCESSING EXAMPLE ===
python3 << 'EOF'
from news_cn.batch_processor import BatchGDELTProcessor
from news_cn.downloader import GDELTDownloader
from datetime import datetime

# Setup optimized processor
downloader = GDELTDownloader()
processor = BatchGDELTProcessor(download_workers=10, threads=4)

# Get files for date range
file_list = downloader.get_available_files(
    start_date=datetime(2026, 1, 1),
    data_types=['export']
)

# Process all days (10x faster than sequential)
results = processor.process_all_days(file_list)
print(f"Processed {len(results)} days")
EOF
```

## 📊 Data Structure

### Raw Events (from GDELT)
- **Location**: `data/parquet/events/year=YYYY/month=MM/day=DD/`
- **Format**: Partitioned Parquet
- **Columns**: 63 (many with NULLs)
- **Country Codes**: Mixed (SAU for actors, SA for geography)

### Cleaned Events
- **Location**: `data/cleaned/saudi_events_cleaned.parquet`
- **Columns**: 30 (essential only)
- **Country Codes**: Normalized to 2-letter (SA, AE, QA)
- **Filtering**: Only events with valid source URLs

### Enriched Articles
- **Location**: `data/enriched_articles_advanced.json`
- **Format**: JSON array
- **Contains**: GDELT metadata + full article text
- **Fields**: date, actors, location, tone, url, title, content, author, publish_date

## 🔑 Key Country Codes

| 3-Letter (Actors) | 2-Letter (Geo) | Country |
|-------------------|----------------|---------|
| SAU | SA | Saudi Arabia |
| ARE | AE | UAE |
| QAT | QA | Qatar |
| YEM | YE | Yemen |
| PAK | PK | Pakistan |
| EGY | EG | Egypt |
| USA | US | United States |

## 💡 Quick Tips

1. **Incremental Updates**: Just run `uv run news-cn` daily - it skips processed files
2. **Country Filtering**: Current data uses `ActionGeo_CountryCode = 'SA'` (events IN Saudi Arabia)
3. **Data Quality**: ~63% of events have Saudi Arabia as location, ~38% have Saudi actors
4. **NULL Values**: Many events lack actor names/countries - this is normal in GDELT
5. **Article Scraping**: Use nodriver (free) first, Firecrawl (paid) for fallback

## 🐛 Common Issues

| Issue | Solution |
|-------|----------|
| "No files found" | Check date range in config.py |
| "nodriver not available" | Run `uv add nodriver` |
| "Rate limit exceeded" | Use advanced scraper instead of Jina |
| "All methods failed" | Site blocks automated access, try Firecrawl |
| "Memory error" | Reduce DUCKDB_MEMORY_LIMIT in config.py |

## 📈 Performance Benchmarks

### Data Volume (Jan 2026)
- **Total Events**: 18,779
- **After Cleaning**: 11,883 (37% removed - no valid URLs)
- **Top Actor Country**: SAU (7,158 events = 38.1%)
- **Geographic SA Events**: 11,883 (63.3%)
- **File Size**: ~60 KB per day (compressed)

### Processing Speed
- **Sequential Pipeline**: ~5-10 minutes per day (96 files)
- **Batch Processor**: ~30-60 seconds per day (96 files)
- **Speedup**: **~10x faster with batch processing**
- **Article Scraping**: 2-3 sec/article (nodriver), 5-10 sec (Firecrawl)

## 🔄 Typical Workflow

```bash
# Day 1: Initial setup
uv sync
uv add nodriver                # For article scraping
uv run news-cn                 # Collect all data from 2026-01-01

# Day 2-N: Daily updates
uv run news-cn                 # Get new data (incremental)
uv run news-cn-scrape-advanced 50  # Scrape recent articles

# Weekly: Data cleaning
uv run news-cn-clean           # Clean & normalize

# Monthly: Stats & maintenance
uv run news-cn-tools stats     # Check processing status
uv run news-cn-tools consolidate  # Manually consolidate if needed
```

## 🎯 Installation Profiles

```bash
# Minimal (core pipeline only)
uv sync

# Standard (with scraping)
uv sync
uv add nodriver

# Full (with all features)
uv sync --extra dev --extra scraping
uv add nodriver firecrawl-py

# With BigQuery
uv sync --extra bigquery
```

## 📁 Directory Structure

```
news-cn/
├── data/
│   ├── parquet/events/          # Raw GDELT data (partitioned)
│   ├── cleaned/                 # Cleaned & normalized data
│   ├── enriched_articles*.json  # Articles with full text
│   └── .pipeline_state.json     # Processing state (don't delete!)
├── src/news_cn/
│   ├── cli.py                   # Main pipeline
│   ├── processor.py             # GDELT processor (sequential)
│   ├── batch_processor.py       # Optimized batch processor (10x faster)
│   ├── article_scraper.py       # Basic scraper (Jina)
│   ├── advanced_scraper.py      # Hybrid scraper (nodriver+Firecrawl)
│   └── data_cleaner.py          # Data normalization
├── examples/
│   ├── complete_pipeline_demo.py
│   └── article_enrichment_example.py
└── *.md                         # Documentation
```

## 🌐 External Resources

- **Nodriver**: https://github.com/ultrafunkamsterdam/nodriver
- **Firecrawl**: https://firecrawl.dev
- **GDELT Docs**: http://data.gdeltproject.org/documentation/
- **DuckDB**: https://duckdb.org/docs/

---

Last updated: 2026-01-27
