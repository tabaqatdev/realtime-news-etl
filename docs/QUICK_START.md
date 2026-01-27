# News-CN Quick Start Guide

## 🚀 Simplest Commands (Smart Defaults Enabled)

### ✨ Full Pipeline with Defaults

```bash
# Geographic enrichment + Deduplication enabled by default
# Scrapes 50 articles, starts from 2026-01-01 (or today if after 2026)
news-cn --country SA
```

**What it does:**

- ✅ Collects GDELT data from 2026-01-01 to today
- ✅ Cleans data (removes nulls, basic duplicates)
- ✅ **Smart deduplication** - keeps best record per URL (enabled by default)
- ✅ **Geographic enrichment** - adds nearest cities for all 3 coordinate sets (enabled by default)
- ✅ Scrapes 50 article titles, content, and authors
- ✅ Merges everything into comprehensive final dataset

**Output:** `data/parquet/cleaned/final_enriched.parquet`

---

## 📋 Common Use Cases

### 1. Unlimited Article Scraping

```bash
# Scrape ALL unique URLs (~4,000 articles, takes ~2-3 hours)
news-cn --country SA --scrape-limit 99999
```

### 2. Quick Run Without Scraping

```bash
# Get cleaned, deduplicated, geo-enriched data only (~30 seconds)
news-cn --country SA --no-scrape
```

### 3. Maximum Speed (No Enrichments)

```bash
# Skip geo-enrichment and deduplication for fastest processing
news-cn --country SA --no-geo --no-dedupe --no-scrape
```

### 4. Custom Date Range

```bash
# Collect data from specific date
news-cn --country SA --start-date 2026-01-15
```

### 5. Different Country

```bash
# United States
news-cn --country US --scrape-limit 100

# United Kingdom
news-cn --country UK --scrape-limit 100
```

---

## 📊 What You Get

### Smart Defaults (NEW!)

- ✅ **Geo-enrichment**: Enabled by default (use `--no-geo` to disable)
- ✅ **Deduplication**: Enabled by default (use `--no-dedupe` to disable)
- ✅ **Start date**: Automatically set to 2026-01-01 (or today's date if we're past 2026)
- ✅ **Country**: Saudi Arabia (SA)
- ✅ **Scrape limit**: 50 articles (use `--scrape-limit 99999` for unlimited)

### Output Files

1. **`data/parquet/cleaned/final_enriched.parquet`** ⭐ **MAIN OUTPUT**
   - Comprehensive dataset with everything:
     - Original GDELT fields (58 columns)
     - Geographic enrichment (12 columns)
     - Article content (6 columns)
     - Quality score (1 column)
   - Total: 77+ columns
   - One record per unique URL (deduplicated)
   - Average quality score: 98+

2. **`data/parquet/cleaned/deduplicated_events.parquet`**
   - Deduplicated events (one per URL)
   - Includes quality_score column

3. **`data/parquet/cleaned/geo_enriched.parquet`**
   - Geographic enrichment only
   - Nearest cities for ActionGeo, Actor1Geo, Actor2Geo

4. **`data/articles/enriched_articles.parquet`** + **`.json`**
   - Scraped articles in both formats

---

## 🎯 Typical Results

### Deduplication Impact

- **Before**: ~19,000 records (4,000 unique URLs)
- **After**: ~4,000 records (one per URL)
- **Duplicates removed**: ~15,000 (78% reduction!)
- **Quality**: 94% excellent, 6% good

### Geographic Enrichment

- **Coverage**: 80-85% of events matched to cities
- **Database**: 33,227 cities from 244 countries
- **Quality indicators**: Distinguishes precise coordinates from country centroids

### Article Scraping (with unlimited limit)

- **Success rate**: ~90% (Trafilatura + Newspaper4k + Playwright)
- **Speed**: ~2 seconds per article
- **Expected articles**: ~3,600-3,800 from 4,000 URLs
- **Content**: Full article text (avg 3,000-6,000 chars), title, author

---

## 💡 Tips

### Performance

- Use `--no-scrape` for fastest processing (~30 seconds)
- Start with `--scrape-limit 100` to test, then increase
- Run unlimited scraping overnight: `--scrape-limit 99999`

### Quality

- Keep deduplication enabled (default) for best data quality
- Keep geo-enrichment enabled (default) for location analysis
- Quality score helps identify most complete records
- Failed scrapes (403, SSL errors, timeouts) are automatically excluded from output

### Resume Capability ⭐ NEW!

- **Automatic resume**: If you cancel (Ctrl+C) or the process crashes, just run the same command again
- Progress is saved every 10 articles automatically
- Already-scraped URLs are skipped (no duplicate work)
- Example: Cancel at 150/10546, restart, continues from 151
- Perfect for long-running unlimited scraping jobs

### Storage

- Parquet files use ZSTD compression (very efficient)
- JSON files provided for easy inspection
- Final dataset typically 5-20 MB compressed

---

## 📖 Sample Queries

### Query 1: High-Quality Events with Articles

```sql
SELECT
    SQLDATE,
    Actor1Name,
    Actor2Name,
    NearestCity,
    quality_score,
    ArticleTitle,
    ArticleAuthor,
    LENGTH(ArticleContent) as content_length
FROM 'data/parquet/cleaned/final_enriched.parquet'
WHERE quality_score >= 95
  AND ArticleTitle IS NOT NULL
  AND CoordQuality = 'high'
ORDER BY SQLDATE DESC
LIMIT 20;
```

### Query 2: Geographic Distribution

```sql
SELECT
    NearestCity,
    COUNT(*) as events,
    ROUND(AVG(AvgTone), 2) as avg_tone,
    COUNT(ArticleTitle) as with_articles
FROM 'data/parquet/cleaned/final_enriched.parquet'
WHERE NearestCity IS NOT NULL
GROUP BY NearestCity
ORDER BY events DESC
LIMIT 20;
```

### Query 3: Article Content Analysis

```sql
SELECT
    SQLDATE,
    ArticleTitle,
    ArticleAuthor,
    ArticleScrapeMethod,
    LENGTH(ArticleContent) as content_length,
    SUBSTRING(ArticleContent, 1, 200) as preview
FROM 'data/parquet/cleaned/final_enriched.parquet'
WHERE ArticleTitle IS NOT NULL
ORDER BY SQLDATE DESC
LIMIT 10;
```

---

## 🆘 Help

### View All Options

```bash
news-cn --help
```

### Common Issues

**Issue**: "No such file or directory"

- **Solution**: Make sure you're in the project directory

**Issue**: Scraping takes too long

- **Solution**: Use smaller `--scrape-limit` (default: 50)

**Issue**: Want to skip enrichments for speed

- **Solution**: Use `--no-geo --no-dedupe --no-scrape`

**Issue**: Need data from earlier date

- **Solution**: Use `--start-date YYYY-MM-DD`

---

## 📦 What's New

### Version 0.2.0 - Smart Defaults Update

- ✨ **Geographic enrichment enabled by default** (was opt-in)
- ✨ **Smart deduplication enabled by default** (was opt-in)
- ✨ **Automatic start date**: Uses 2026-01-01 or today's date
- 🚀 **Simpler CLI**: Just `news-cn --country SA` for full pipeline
- 📊 **Quality scoring**: Each record gets 0-100 quality score
- 🎯 **Best record selection**: Automatically picks most complete record per URL
- 🌍 **Enhanced geo-enrichment**: Now covers all 3 GDELT coordinate sets
- 📰 **Modern scraping**: Trafilatura → Newspaper4k → Playwright layered approach

---

## 🎉 One Command to Rule Them All

```bash
# Complete pipeline with unlimited scraping
news-cn --country SA --scrape-limit 99999
```

This single command:

- ✅ Collects all available data from 2026 onwards
- ✅ Cleans and deduplicates (~78% reduction)
- ✅ Enriches with geographic data (~83% coverage)
- ✅ Scrapes ALL unique articles (~3,600-3,800 articles)
- ✅ Produces comprehensive final dataset ready for analysis

**Time**: ~2-3 hours for full run with unlimited scraping
**Output**: Single Parquet file with everything you need! 🚀
