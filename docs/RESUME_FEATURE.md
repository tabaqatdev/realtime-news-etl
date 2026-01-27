# Resume Capability & Failed URL Tracking

## ✨ Features (Updated 2026-01-28)

### 1. Automatic Resume on Interruption

If you cancel the scraping process (Ctrl+C) or it crashes, **just run the same command again** and it will continue from where it stopped.

**How it works:**
- Progress is automatically saved every 10 attempts (successful or failed)
- When you restart, it reads both successful scrapes AND failed attempts
- **Already-attempted URLs are skipped** (both successful and failed)
- Only new URLs are processed
- No duplicate work, no wasted time on known failures

**Example:**
```bash
# Start scraping 10,546 URLs
uv run news-cn --country SA --scrape-limit 99999

# ... scraping 150/10546 (100 success, 50 failed) ...
# Press Ctrl+C to cancel

# Later, run the EXACT SAME command
uv run news-cn --country SA --scrape-limit 99999

# Output will show:
# 🔄 Resume mode: Skipping 150 already-attempted URLs (success + failed)
# 📊 Progress: 150 attempted, 10396 remaining, 10546 total
```

### 2. Failed URLs Tracked and Skipped

**CRITICAL FEATURE**: Failed URLs are now tracked and **skipped on resume** to save time.

**Why this matters:**
- You had 9704 remaining URLs, but many were the same failed URLs being retried
- Same 403 error URL was being attempted multiple times (wasting 3+ seconds each)
- SSL error URLs were being retried even though they'll never work
- This wastes hours on long scraping runs

**Now implemented:**
- ✅ Failed URLs are tracked in `data/articles/failed_urls.parquet`
- ✅ On resume, both successful AND failed URLs are skipped
- ✅ No more retrying known failures (403, SSL, 404, timeouts)
- ✅ Saves hours on long scraping runs

**What counts as a failed URL:**
- 403 Forbidden errors (anti-bot protection)
- SSL/TLS errors (certificate issues)
- 404 Not Found errors
- Connection timeouts
- Connection resets
- All other network/scraping errors

### 3. Failed Scrapes Automatically Excluded from Output

**Failed scrapes are NOT included in the final output files:**
- `enriched_articles.parquet` contains ONLY successful scrapes
- `final_enriched.parquet` contains ONLY events with article content
- Failed URLs are tracked separately in `failed_urls.parquet`

**What you'll see in logs:**
```
✗ All methods failed: https://example.com/article...
✗ SSL error (skipped): https://article.wn.com/...
```

**These failed URLs are:**
- ✅ Logged with warning message
- ✅ Tracked in failed_urls.parquet
- ✅ Automatically excluded from output
- ✅ Skipped on future resumes
- ✅ Will NOT appear in final_enriched.parquet

---

## 📊 Statistics You'll See

### During Scraping
```
[1/9704] Fetching: https://jordantimes.com/...
✗ All methods failed: https://jordantimes.com/...

[2/9704] Fetching: https://article.wn.com/...
✗ SSL error (skipped): https://article.wn.com/...

[3/9704] Fetching: https://jordantimes.com/...
✗ All methods failed: https://jordantimes.com/...

[38/9704] Fetching: https://www.dailymail.co.uk/...
✓ Trafilatura: https://www.dailymail.co.uk/...

💾 Progress saved: 5 articles scraped so far
📝 Failed URLs tracked: 33 attempts will be skipped on resume
```

### On Resume
```
🔄 Resume mode: Skipping 235 already-attempted URLs (success + failed)
📊 Progress: 235 attempted, 10311 remaining, 10546 total

# Notice: previously failed URLs are NOT attempted again
# Example: jordantimes.com (403) was attempted once, now skipped forever
```

### After Completion
```
✅ Successfully scraped 8,123/10,546 articles (2,423 failed)
📝 Saved 2,423 failed URLs to skip on future resumes

Success rate: 77.0% (8,123 successful out of 10,546 attempted)
Failed: 2,423 URLs (skipped on future runs)
```

---

## 🎯 Use Cases

### Long-Running Jobs
Perfect for scraping thousands of articles over hours:
```bash
# Start unlimited scraping
uv run news-cn --country SA --scrape-limit 99999

# Cancel anytime with Ctrl+C
# Resume anytime by running the same command
# Failed URLs won't be retried - saves time!
```

### Handling Failures Efficiently
No wasted time on known failures:
- Failed URLs tracked automatically
- Resume skips both successful and failed URLs
- Only new/untried URLs are attempted
- Saves hours on long runs

### Incremental Updates
Run the pipeline daily to collect new articles:
```bash
# Day 1: Scrape everything
uv run news-cn --country SA --scrape-limit 99999
# Result: 8,123 success, 2,423 failed

# Day 2: Only new URLs will be scraped
uv run news-cn --country SA --scrape-limit 99999
# Skips all 10,546 URLs from Day 1 (both success + failed)
# Only scrapes new URLs from fresh GDELT data
```

---

## 🔍 Technical Details

### Dual Tracking System

**1. Successful Scrapes** (`enriched_articles.parquet`):
```python
# Only successful articles are saved here
if article.get("status") == "success":
    enriched.append({
        "url": url,
        "title": article["title"],
        "content": article["content"],
        ...
    })
```

**2. Failed Attempts** (`failed_urls.parquet`):
```python
# All failures are tracked here
else:
    failed_urls.append({
        "url": url,
        "status": article.get("status"),  # e.g., "ssl_error", "http_403"
        "error": article.get("error"),
    })
```

### Resume Detection Logic
```python
# Load both successful and failed URLs
already_attempted = set()

# Load successful scrapes
if Path(output_file).exists():
    existing = duckdb.execute(f"SELECT DISTINCT url FROM '{output_file}'")
    already_attempted.update(existing)

# Load failed attempts
if Path(failed_file).exists():
    failed = duckdb.execute(f"SELECT DISTINCT url FROM '{failed_file}'")
    already_attempted.update(failed)

# Skip all previously attempted URLs
rows = [row for row in all_rows if row.url not in already_attempted]
```

### Incremental Save Logic
- Every 10 attempts (successful or failed) triggers a save
- Successful articles → `enriched_articles.parquet`
- Failed URLs → `failed_urls.parquet`
- Both files are used on resume to skip already-attempted URLs

---

## 💾 Output Files

### Main Output (Successful Scrapes Only)
- `data/articles/enriched_articles.parquet` - ONLY successful scrapes
- `data/articles/enriched_articles.json` - Same data, human-readable

### Failed URL Tracking
- `data/articles/failed_urls.parquet` - All failed attempts (for resume skipping)
- Contains: url, status (ssl_error/http_403/timeout/etc.), error message
- **Purpose**: Skip these on future runs to save time

### Final Merged Output
- `data/parquet/cleaned/final_enriched.parquet` - Events + Articles + Geo data
- Contains ONLY events with successfully scraped articles (INNER JOIN)

---

## 🚀 Best Practices

1. **For unlimited scraping**: Start with `--scrape-limit 99999` and let it run
2. **If interrupted**: Just run the same command again - resumes automatically
3. **Monitor progress**: Check logs for "💾 Progress saved" and "📝 Failed URLs tracked"
4. **Check success rate**: After completion, review the success/failure counts
5. **No cleanup needed**: Failed scrapes are excluded, but tracked for skipping

---

## 🆘 Troubleshooting

**Q: How do I know if resume is working?**
A: Look for this log line at the start:
```
🔄 Resume mode: Skipping 235 already-attempted URLs (success + failed)
📊 Progress: 235 attempted, 10311 remaining, 10546 total
```

**Q: What if I want to start fresh (retry failed URLs)?**
A: Delete both output files before running:
```bash
rm data/articles/enriched_articles.parquet
rm data/articles/failed_urls.parquet
uv run news-cn --country SA --scrape-limit 99999
```

**Q: How often is progress saved?**
A: Every 10 attempts (both successful articles and failed URLs)

**Q: Will failed URLs be retried on resume?**
A: **NO** (changed in 2026-01-28 update). Failed URLs are now tracked and skipped on resume to save time. Before this update, they were retried every time, wasting hours.

**Q: Can I see which URLs failed?**
A: Yes, two ways:
1. Check logs for lines with "✗ All methods failed" or "✗ SSL error"
2. Query the failed_urls.parquet file:
   ```bash
   duckdb -c "SELECT status, COUNT(*) as count
              FROM 'data/articles/failed_urls.parquet'
              GROUP BY status ORDER BY count DESC"
   ```

**Q: Why am I seeing the same URL failing multiple times in logs?**
A: This indicates you're looking at OLD logs before the 2026-01-28 update. After the update, each URL is only attempted ONCE across all runs. If you're still seeing this, make sure you have the latest code and delete old output files to start fresh.

---

## 📈 Performance Impact

**Before (retrying failed URLs):**
- 10,546 total URLs
- 235 already scraped (successful)
- 9,704 remaining
- BUT many of the 9,704 are the same failed URLs being retried
- Example: jordantimes.com (403) attempted 10+ times = 30+ seconds wasted

**After (skipping failed URLs):**
- 10,546 total URLs
- 150 successful (skipped)
- 85 failed (skipped)
- 10,311 truly new URLs remaining
- **Time saved**: ~85 failed URLs × 3-5 seconds = 4-7 minutes per resume
- **Over multiple resumes**: Hours saved on long scraping jobs

Perfect for long-running scraping jobs! 🚀
