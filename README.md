# GDELT News Pipeline

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/duckdb-1.4+-yellow.svg)](https://duckdb.org/)

Collect and process GDELT 2.0 news data with daily partitioned Parquet output. Designed for automated daily runs via GitHub Actions with S3 upload.

## Pipeline

```
GDELT 15-min files → Download → Clean → Deduplicate → Geo-Enrich → Scrape Articles → Daily Parquet
```

Each day produces one enriched Parquet file containing all events LEFT JOINed with scraped article content.

## Quick Start

```bash
uv sync
uv run playwright install chromium --with-deps
```

### Daily mode (recommended)

```bash
# Process yesterday (default)
uv run news-cn

# Backfill from Jan 1 to today
uv run news-cn --start-date 2026-01-01

# Process a specific day
uv run news-cn --start-date 2026-02-07 --end-date 2026-02-07

# Different country
uv run news-cn --country AE --start-date 2026-02-01
```

Daily mode is idempotent — it skips days that already have output files. Safe to re-run for gap-filling.

### Full mode (legacy)

```bash
# All data into one combined file
uv run news-cn --mode full --start-date 2026-01-01
```

## Output Structure

Same hierarchy locally and on S3:

```
data/output/
└── country=SA/
    └── year=2026/
        ├── 2026_01_01.parquet
        ├── 2026_01_02.parquet
        └── ...
```

Each daily file contains all columns from GDELT events plus article columns (`ArticleTitle`, `ArticleContent`, `ArticleAuthor`, `ArticlePublishDate`, `ArticleContentLength`, `ArticleScrapeMethod`). Events without scraped articles have NULL article columns.

## Querying Output

```bash
# Count events for a day
duckdb -c "SELECT count(*) FROM 'data/output/country=SA/year=2026/2026_02_07.parquet'"

# Events with articles
duckdb -c "
  SELECT SQLDATE, Actor1Name, ArticleTitle, SOURCEURL
  FROM 'data/output/country=SA/**/*.parquet'
  WHERE ArticleTitle IS NOT NULL
  ORDER BY SQLDATE DESC
  LIMIT 10
"

# Query across all days
duckdb -c "
  SELECT count(*) as total, count(ArticleTitle) as with_articles
  FROM 'data/output/country=SA/**/*.parquet'
"
```

## CLI Options

```
usage: news-cn [options]

--country        Country code (default: SA)
--start-date     Start date YYYY-MM-DD (default: 2026-01-01)
--end-date       End date YYYY-MM-DD (default: yesterday in daily mode)
--output-dir     Output directory (default: data)
--mode           daily (per-day files) or full (one combined file)
--scrape-limit   Max articles to scrape per day (default: 500)
--strategy       batch or streaming (default: batch)
--no-scrape      Skip article scraping (full mode only)
--no-geo         Disable geographic enrichment (full mode only)
--no-dedupe      Disable deduplication (full mode only)
```

## Other Tools

```bash
# Standalone scraper
uv run news-cn-scrape

# Data cleaning
uv run news-cn-clean

# Geographic correction
uv run news-cn-geo

# Diagnostics
uv run news-cn-diagnose
```

## GitHub Actions

The workflow at `.github/workflows/daily-pipeline.yml` runs the pipeline daily at midnight UTC and uploads output to S3.

### Setup

1. Add repository secrets:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_DEFAULT_REGION`

2. Add repository variables:
   - `S3_BUCKET` — bucket name (e.g. `us-west-2.opendata.source.coop`)
   - `S3_PREFIX` — key prefix (e.g. `tabaqat/gdelt-sa`)

### Usage

- **Daily cron**: Automatically processes yesterday's data
- **Backfill**: Trigger manually with `start_date=2026-01-01` to fill all missing days
- **Single day**: Trigger with both `start_date` and `end_date` set to the same date

## Article Scraping

Articles are scraped using a layered fallback strategy:

1. **Trafilatura** — fastest, highest accuracy (F1: 0.958)
2. **Newspaper4k** — good fallback
3. **Playwright** — handles JS-rendered pages and anti-bot detection

## Development

```bash
uv run ruff check src/       # Lint
uv run ruff format src/      # Format
uv run pytest                # Test
```

## Data Fields

Each daily Parquet file contains [GDELT 2.0 Event fields](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf) plus:

| Field | Type | Description |
|-------|------|-------------|
| `ArticleTitle` | VARCHAR | Scraped article title |
| `ArticleContent` | VARCHAR | Full article text |
| `ArticleAuthor` | VARCHAR | Article author |
| `ArticlePublishDate` | VARCHAR | Publication date |
| `ArticleContentLength` | BIGINT | Content length in chars |
| `ArticleScrapeMethod` | VARCHAR | Method used (trafilatura/newspaper4k/playwright) |

Key GDELT fields: `GLOBALEVENTID`, `SQLDATE`, `Actor1Name`, `Actor2Name`, `EventCode`, `GoldsteinScale`, `AvgTone`, `ActionGeo_FullName`, `SOURCEURL`.

## License

MIT
