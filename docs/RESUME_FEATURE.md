# Resume Capability & Failed Scrape Filtering

## ✨ New Features (2026-01-27)

### 1. Automatic Resume on Interruption

If you cancel the scraping process (Ctrl+C) or it crashes, **just run the same command again** and it will continue from where it stopped.

**How it works:**
- Progress is automatically saved every 10 successfully scraped articles
- When you restart, it reads the existing output file
- Already-scraped URLs are identified and skipped
- Only remaining URLs are processed
- No duplicate work, no lost progress

**Example:**
```bash
# Start scraping 10,546 URLs
uv run news-cn --country SA --scrape-limit 99999

# ... scraping 150/10546 ...
# Press Ctrl+C to cancel

# Later, run the EXACT SAME command
uv run news-cn --country SA --scrape-limit 99999

# Output will show:
# 🔄 Resume mode: Found 150 already-scraped URLs
# 📊 Progress: 150 done, 10396 remaining, 10546 total
```

### 2. Failed Scrapes Automatically Excluded

**Failed scrapes are NOT included in the final output:**
- 403 Forbidden errors (anti-bot protection)
- SSL/TLS errors (certificate issues)
- Connection timeouts
- Connection resets
- Any other network errors

**What you'll see in logs:**
```
✗ All methods failed: https://example.com/article...
```

**These failed URLs are:**
- ✅ Logged with warning message
- ✅ Automatically excluded from output parquet/JSON
- ✅ NOT counted in success statistics
- ✅ Will NOT appear in final_enriched.parquet

**Only successful scrapes are saved** (status == "success")

---

## 📊 Statistics You'll See

### During Scraping
```
[139/10546] Fetching: https://article.wn.com/...
✗ All methods failed: https://article.wn.com/...

[140/10546] Fetching: https://english.aawsat.com/...
✓ Trafilatura: https://english.aawsat.com/...

💾 Progress saved: 130 articles scraped so far
```

### After Completion
```
✅ Scraped 9,847 new articles → data/articles/enriched_articles.parquet
📊 Total articles in database: 9,847

Success rate: 93.4% (9,847 successful out of 10,546 attempted)
Failed: 699 URLs (403 errors, SSL issues, timeouts)
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
```

### Handling Failures Gracefully
No need to worry about failed URLs:
- Pipeline continues despite failures
- Final output contains only clean, successful scrapes
- No manual cleanup needed

### Incremental Updates
Run the pipeline daily to collect new articles:
```bash
# Day 1: Scrape everything
uv run news-cn --country SA --scrape-limit 99999

# Day 2: Only new URLs will be scraped
uv run news-cn --country SA --scrape-limit 99999
```

---

## 🔍 Technical Details

### Incremental Save Logic
- Every 10 successful scrapes triggers an incremental save
- Uses DuckDB's UNION ALL + DISTINCT to merge with existing data
- Deduplication by URL ensures no duplicates

### Resume Detection
- Checks if `data/articles/enriched_articles.parquet` exists
- Loads all URLs from existing file into a set
- Filters input URLs to exclude already-scraped ones
- Logs: "Resume mode: Found X already-scraped URLs"

### Failure Filtering
```python
# Only append successful scrapes
if article and article.get("status") == "success":
    enriched.append({...})
```

Failed articles have status like:
- `"status": "all_failed"` - All 3 methods failed
- `"status": "failed"` - Method-specific failure
- `"status": "no_content"` - Fetch succeeded but content too short
- `"status": "insufficient_content"` - Content < 100 chars

**None of these are added to the output.**

---

## 💾 Output Files

### Main Output (Parquet + JSON)
`data/articles/enriched_articles.parquet` - Contains ONLY successful scrapes
`data/articles/enriched_articles.json` - Same data, human-readable

### Final Merged Output
`data/parquet/cleaned/final_enriched.parquet` - Events + Articles + Geo data

All outputs contain **only successfully scraped articles** with full content.

---

## 🚀 Best Practices

1. **For unlimited scraping**: Start with `--scrape-limit 99999` and let it run
2. **If interrupted**: Just run the same command again - it will resume
3. **Monitor progress**: Check logs for "💾 Progress saved" messages
4. **Check success rate**: After completion, review the success/failure counts
5. **No cleanup needed**: Failed scrapes are automatically excluded

---

## 🆘 Troubleshooting

**Q: How do I know if resume is working?**
A: Look for this log line at the start:
```
🔄 Resume mode: Found X already-scraped URLs
📊 Progress: X done, Y remaining, Z total
```

**Q: What if I want to start fresh?**
A: Delete the output file before running:
```bash
rm data/articles/enriched_articles.parquet
uv run news-cn --country SA --scrape-limit 99999
```

**Q: How often is progress saved?**
A: Every 10 successfully scraped articles

**Q: Will failed URLs be retried on resume?**
A: No. Failed URLs are not saved, so they will be attempted again on the next run. If they fail again, they're excluded again.

**Q: Can I see which URLs failed?**
A: Yes, check the logs for lines with "✗ All methods failed"
