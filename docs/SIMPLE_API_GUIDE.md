# Simple API Guide - Beginner-Friendly GDELT Collection

## 🎯 Quick Start (3 Lines of Code)

```python
from news_cn import collect_news

# Collect all Saudi Arabia news from Jan 1, 2026
results = collect_news()
```

That's it! The package handles everything automatically with smart defaults.

## 📚 API Options

### Option 1: Simple Function (Easiest)

```python
from news_cn import collect_news

# Collect news for different country
results = collect_news(country="AE", start_date="2026-01-15")

# Use memory-efficient streaming
results = collect_news(strategy="streaming")

# Custom output directory
results = collect_news(output_dir="my_data/parquet")
```

**Parameters:**
- `country` (str): 2-letter country code (default: "SA")
- `start_date` (datetime|str): Start date (default: "2026-01-01")
- `data_types` (list): Data types to collect (default: ["export"])
- `output_dir` (str): Output directory (default: "data/parquet")
- `strategy` (str): "batch" (fast) or "streaming" (memory efficient)

**Returns:** Dictionary mapping date → Path to parquet file

---

### Option 2: Fluent API (Most Flexible)

```python
from news_cn import SimplePipeline

# Chain configuration methods
pipeline = (SimplePipeline()
    .for_country("SA")
    .from_date("2026-01-01")
    .to_date("2026-01-31")
    .use_batch_processing()
    .output_to("data/parquet")
    .run())

# Check results
print(f"Processed {len(pipeline.results)} days")

# Query the collected data
recent_events = pipeline.query(limit=20)
for event in recent_events:
    print(f"{event['SQLDATE']}: {event['Actor1Name']} -> {event['Actor2Name']}")
```

**Available methods:**
- `.for_country(code)` - Set target country
- `.from_date(date)` - Set start date
- `.to_date(date)` - Set end date (optional)
- `.use_batch_processing()` - Fast parallel processing
- `.use_streaming()` - Memory-efficient processing
- `.output_to(dir)` - Set output directory
- `.run()` - Execute the pipeline
- `.query(limit=10)` - Query collected data

---

### Option 3: Convenience Shortcuts

```python
from news_cn import collect_saudi_news, collect_uae_news

# Get last 7 days of Saudi news
results = collect_saudi_news(days_back=7)

# Get last 30 days of UAE news
results = collect_uae_news(days_back=30)
```

---

## 📊 Querying Collected Data

### Quick Query

```python
from news_cn import query_news

# Get 10 most recent events
events = query_news(country="SA", limit=10)

for event in events:
    print(f"""
    Date: {event['SQLDATE']}
    Actors: {event['Actor1Name']} → {event['Actor2Name']}
    Location: {event['Location']}
    Tone: {event['AvgTone']}
    URL: {event['URL']}
    """)
```

### Custom DuckDB Queries

```python
import duckdb

conn = duckdb.connect()
result = conn.execute("""
    SELECT
        SQLDATE,
        Actor1Name,
        Actor2Name,
        COUNT(*) as event_count
    FROM 'data/parquet/events/**/*.parquet'
    WHERE ActionGeo_CountryCode = 'SA'
    GROUP BY SQLDATE, Actor1Name, Actor2Name
    ORDER BY event_count DESC
    LIMIT 20
""").fetchall()
```

---

## 🎨 Complete Examples

### Example 1: Basic Collection

```python
from news_cn import collect_news

# Collect all Saudi news since Jan 1
results = collect_news(
    country="SA",
    start_date="2026-01-01"
)

print(f"✅ Collected {len(results)} days of news")
for date, path in results.items():
    print(f"  {date}: {path}")
```

### Example 2: Multi-Country Analysis

```python
from news_cn import collect_news

countries = ["SA", "AE", "QA", "KW", "BH", "OM"]

for country in countries:
    print(f"Collecting {country}...")
    results = collect_news(
        country=country,
        start_date="2026-01-01",
        output_dir=f"data/{country}"
    )
    print(f"  ✓ {len(results)} days")
```

### Example 3: Date Range Collection

```python
from news_cn import SimplePipeline

pipeline = (SimplePipeline()
    .for_country("SA")
    .from_date("2026-01-01")
    .to_date("2026-01-31")
    .use_batch_processing()
    .run())

# Analyze results
events = pipeline.query(limit=100)
print(f"Total events: {len(events)}")

# Find most active actors
from collections import Counter
actors = [e['Actor1Name'] for e in events if e['Actor1Name']]
top_actors = Counter(actors).most_common(10)
print("Top actors:", top_actors)
```

### Example 4: Memory-Efficient Processing

```python
from news_cn import collect_news

# For systems with limited RAM, use streaming
results = collect_news(
    country="SA",
    start_date="2026-01-01",
    strategy="streaming"  # Uses less memory
)
```

---

## 🔧 Advanced Usage

If you need more control, use the core components directly:

```python
from news_cn import Config, GDELTDownloader, GDELTProcessor

# Custom configuration
config = Config()
config.DUCKDB_MEMORY_LIMIT = "8GB"  # Use more RAM
config.DOWNLOAD_WORKERS = 20        # More parallel downloads
config.TARGET_COUNTRY_CODE = "AE"

# Initialize components
downloader = GDELTDownloader(config=config)
processor = GDELTProcessor(strategy="batch", config=config)

# Get and process files
file_list = downloader.get_available_files(
    start_date=config.START_DATE,
    data_types=["export"]
)

results = processor.process_all_days(file_list)
```

---

## 💡 Smart Defaults

The simple API uses these sensible defaults:

| Setting | Default Value | Why? |
|---------|---------------|------|
| Country | "SA" (Saudi Arabia) | Original project focus |
| Start Date | 2026-01-01 | Recent data collection |
| Strategy | "batch" | 10x faster than streaming |
| Memory Limit | 4GB | Works on most machines |
| Download Workers | 10 | Good balance speed/resources |
| Threads | 4 | Standard CPU core count |
| Compression | ZSTD | Best parquet compression |
| Data Types | ["export"] | Events only (most useful) |

---

## 🆚 Comparison: Simple vs Advanced API

### Simple API (Recommended for Most Users)

```python
from news_cn import collect_news

results = collect_news()  # One line!
```

**Pros:**
- ✅ Minimal code
- ✅ Smart defaults
- ✅ Easy to understand
- ✅ Perfect for beginners

**Cons:**
- ⚠️  Less customization

---

### Advanced API (For Complex Workflows)

```python
from news_cn import Config, GDELTDownloader, GDELTProcessor
from news_cn.schemas import SchemaFactory
from news_cn.duckdb_utils import DuckDBQueryBuilder

config = Config()
# ... 10+ lines of setup ...
```

**Pros:**
- ✅ Full control
- ✅ Custom schemas
- ✅ Complex queries
- ✅ Pipeline customization

**Cons:**
- ⚠️  More code to write
- ⚠️  Steeper learning curve

---

## 📖 Next Steps

1. **Try the basic example** above
2. **Check QUICK_REFERENCE.md** for CLI commands
3. **Read README.md** for architecture details
4. **Explore examples/** directory for more use cases

---

## 🐛 Troubleshooting

### "No files found"
```python
# Check your date range
results = collect_news(start_date="2026-01-01")  # Make sure date has data
```

### "Memory error"
```python
# Use streaming or reduce memory limit
results = collect_news(strategy="streaming")

# Or adjust config
from news_cn import Config
config = Config()
config.DUCKDB_MEMORY_LIMIT = "2GB"
```

### "Connection timeout"
```python
# Increase timeout
from news_cn import Config
config = Config()
config.DOWNLOAD_TIMEOUT = 120  # 2 minutes
```

---

**Last updated:** 2026-01-27
**Version:** 0.2.0
