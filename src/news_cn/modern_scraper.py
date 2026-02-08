"""
Modern Article Scraper with Layered Hybrid Approach (2026)
Uses lightweight extractors first, then browser automation as fallback
Strategy: Trafilatura → Newspaper4k → Playwright
"""

import asyncio
import logging
import warnings
from pathlib import Path

import duckdb

# Suppress urllib3 retry warnings - we handle retries ourselves with fast-fail
from urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings("ignore", category=InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

# Optional imports - install only if needed
try:
    import trafilatura

    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False
    logger.info("trafilatura not installed. Install with: uv add trafilatura")

try:
    from newspaper import Article as Newspaper4kArticle

    NEWSPAPER4K_AVAILABLE = True
except ImportError:
    NEWSPAPER4K_AVAILABLE = False
    logger.info("newspaper4k not installed. Install with: uv add newspaper4k")

try:
    from playwright.async_api import async_playwright

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.info(
        "playwright not installed. Install with: uv add playwright && playwright install chromium"
    )


class ModernArticleScraper:
    """
    Layered article scraper with 3 strategies ordered by speed/reliability:
    1. Trafilatura (fastest, F1: 0.958, no JS support)
    2. Newspaper4k (fast, F1: 0.949, better than newspaper3k)
    3. Playwright (slowest but handles JS, anti-bot detection)
    """

    def __init__(self):
        """Initialize modern scraper with available libraries"""
        self.methods_available = []

        if TRAFILATURA_AVAILABLE:
            self.methods_available.append("trafilatura")
        if NEWSPAPER4K_AVAILABLE:
            self.methods_available.append("newspaper4k")
        if PLAYWRIGHT_AVAILABLE:
            self.methods_available.append("playwright")

        if not self.methods_available:
            logger.warning("No scraping libraries available! Install at least one.")
        else:
            logger.info(f"Available methods: {', '.join(self.methods_available)}")

    def fetch_with_trafilatura(self, url: str, timeout: int = 5) -> dict | None:
        """
        Fetch article using Trafilatura (F1: 0.958, fastest)

        Uses custom requests session with disabled retries for fast SSL failure.

        Args:
            url: Article URL
            timeout: Request timeout in seconds (default: 5 for fast fail)

        Returns:
            Dictionary with article content or None if failed
        """
        if not TRAFILATURA_AVAILABLE:
            return None

        try:
            logger.debug(f"Attempting Trafilatura fetch: {url}")

            # CRITICAL: Bypass trafilatura.fetch_url() to avoid urllib3 retries
            # Use requests directly with no retries and short timeout
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            # Create session with NO retries
            session = requests.Session()
            retry_strategy = Retry(
                total=0,  # NO retries
                connect=0,
                read=0,
                redirect=2,
                status=0,
                backoff_factor=0,
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            # Fetch with timeout
            response = session.get(
                url, timeout=timeout, verify=False
            )  # Skip SSL verification for speed
            response.raise_for_status()
            downloaded = response.text
            if not downloaded:
                return {
                    "url": url,
                    "status": "failed",
                    "method": "trafilatura",
                    "error": "fetch_failed",
                }

            # Extract with metadata
            result = trafilatura.extract(
                downloaded,
                include_comments=False,
                include_tables=True,
                no_fallback=False,
                favor_precision=True,
                favor_recall=False,
                with_metadata=True,
                output_format="json",
            )

            if not result:
                return {"url": url, "status": "no_content", "method": "trafilatura"}

            # Parse JSON result
            import json

            data = json.loads(result) if isinstance(result, str) else result

            content = data.get("text", "")
            if len(content) < 100:
                return {
                    "url": url,
                    "status": "insufficient_content",
                    "method": "trafilatura",
                    "length": len(content),
                }

            return {
                "url": url,
                "title": data.get("title"),
                "content": content,
                "author": data.get("author"),
                "publish_date": data.get("date"),
                "status": "success",
                "method": "trafilatura",
                "length": len(content),
            }

        except Exception as e:
            import requests

            error_str = str(e)

            # Fast fail on SSL/TLS errors - these now fail immediately (no retries)
            if (
                isinstance(e, requests.exceptions.SSLError)
                or "SSL" in error_str
                or "ssl" in error_str.lower()
            ):
                logger.debug(f"SSL error for {url}: {type(e).__name__}")
                return {
                    "url": url,
                    "status": "ssl_error",
                    "method": "trafilatura",
                    "error": "ssl_error",
                }

            # Handle HTTP errors (403, 404, etc.) - also fast fail
            if isinstance(e, requests.exceptions.HTTPError):
                status_code = e.response.status_code if e.response else "unknown"
                logger.debug(f"HTTP {status_code} for {url}")
                return {
                    "url": url,
                    "status": f"http_{status_code}",
                    "method": "trafilatura",
                    "error": f"http_{status_code}",
                }

            # Handle timeouts - fast fail
            if isinstance(
                e,
                (
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout,
                ),
            ):
                logger.debug(f"Timeout for {url}")
                return {
                    "url": url,
                    "status": "timeout",
                    "method": "trafilatura",
                    "error": "timeout",
                }

            # Other errors
            logger.debug(f"Trafilatura failed for {url}: {type(e).__name__}: {e}")
            return {"url": url, "status": "failed", "method": "trafilatura", "error": str(e)}

    def fetch_with_newspaper4k(self, url: str) -> dict | None:
        """
        Fetch article using Newspaper4k (F1: 0.949, fast, news-optimized)

        Args:
            url: Article URL

        Returns:
            Dictionary with article content or None if failed
        """
        if not NEWSPAPER4K_AVAILABLE:
            return None

        try:
            logger.debug(f"Attempting Newspaper4k fetch: {url}")

            article = Newspaper4kArticle(url)
            article.download()
            article.parse()

            content = article.text
            if len(content) < 100:
                return {
                    "url": url,
                    "status": "insufficient_content",
                    "method": "newspaper4k",
                    "length": len(content),
                }

            return {
                "url": url,
                "title": article.title,
                "content": content,
                "author": ", ".join(article.authors) if article.authors else None,
                "publish_date": str(article.publish_date) if article.publish_date else None,
                "status": "success",
                "method": "newspaper4k",
                "length": len(content),
                "top_image": article.top_image if hasattr(article, "top_image") else None,
            }

        except Exception as e:
            logger.debug(f"Newspaper4k failed for {url}: {e}")
            return {"url": url, "status": "failed", "method": "newspaper4k", "error": str(e)}

    async def fetch_with_playwright(self, url: str, timeout: int = 30000) -> dict | None:
        """
        Fetch article using Playwright (handles JS, anti-bot, slowest)

        Args:
            url: Article URL
            timeout: Timeout in milliseconds

        Returns:
            Dictionary with article content or None if failed
        """
        if not PLAYWRIGHT_AVAILABLE:
            return None

        try:
            logger.debug(f"Attempting Playwright fetch: {url}")

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page = await context.new_page()

                # Navigate with timeout
                await page.goto(url, timeout=timeout, wait_until="domcontentloaded")

                # Wait for content to render
                await page.wait_for_timeout(2000)

                # Extract title
                title = None
                try:
                    title = await page.title()
                    # Also try h1
                    h1 = await page.query_selector("h1")
                    if h1:
                        h1_text = await h1.inner_text()
                        if h1_text and len(h1_text) > len(title or ""):
                            title = h1_text
                except Exception:
                    pass

                # Extract main content - try multiple selectors
                content = ""
                selectors = [
                    "article",
                    "main article",
                    "[role='article']",
                    ".article-content",
                    ".post-content",
                    ".entry-content",
                    "main",
                    ".content",
                ]

                for selector in selectors:
                    try:
                        elem = await page.query_selector(selector)
                        if elem:
                            text = await elem.inner_text()
                            if text and len(text) > 300:
                                content = text.strip()
                                logger.debug(
                                    f"Found content using selector: {selector} ({len(content)} chars)"
                                )
                                break
                    except Exception:
                        continue

                await browser.close()

                if len(content) < 100:
                    return {
                        "url": url,
                        "status": "insufficient_content",
                        "method": "playwright",
                        "title": title,
                        "length": len(content),
                    }

                return {
                    "url": url,
                    "title": title,
                    "content": content,
                    "status": "success",
                    "method": "playwright",
                    "length": len(content),
                }

        except Exception as e:
            logger.debug(f"Playwright failed for {url}: {e}")
            return {"url": url, "status": "failed", "method": "playwright", "error": str(e)}

    async def fetch_article_content(self, url: str) -> dict | None:
        """
        Fetch article content using layered approach:
        1. Try Trafilatura (fastest, highest F1 score)
        2. Fallback to Newspaper4k (fast, news-optimized)
        3. Fallback to Playwright (handles JS/anti-bot)

        Args:
            url: Article URL

        Returns:
            Dictionary with article content or None if all methods failed
        """
        # Layer 1: Trafilatura (fastest, no JS)
        if TRAFILATURA_AVAILABLE:
            result = self.fetch_with_trafilatura(url)
            if result and result["status"] == "success":
                logger.info(f"✓ Trafilatura: {url[:60]}...")
                return result
            # Fast fail on SSL errors - don't try other methods
            if result and result.get("status") == "ssl_error":
                logger.warning(f"✗ SSL error (skipped): {url[:60]}...")
                return {"url": url, "status": "ssl_error"}

        # Layer 2: Newspaper4k (fast, news-optimized)
        if NEWSPAPER4K_AVAILABLE:
            result = self.fetch_with_newspaper4k(url)
            if result and result["status"] == "success":
                logger.info(f"✓ Newspaper4k: {url[:60]}...")
                return result

        # Layer 3: Playwright (slowest, handles JS)
        if PLAYWRIGHT_AVAILABLE:
            result = await self.fetch_with_playwright(url)
            if result and result["status"] == "success":
                logger.info(f"✓ Playwright: {url[:60]}...")
                return result

        logger.warning(f"✗ All methods failed: {url[:60]}...")
        return {"url": url, "status": "all_failed"}

    def merge_articles_with_events(
        self,
        events_parquet: str,
        articles_parquet: str,
        output_parquet: str,
        only_with_articles: bool = False,
    ):
        """
        Merge scraped articles back into the geo-enriched events parquet

        Args:
            events_parquet: Path to geo-enriched events parquet
            articles_parquet: Path to scraped articles parquet
            output_parquet: Path for final merged output
            only_with_articles: If True, only include events with successfully scraped articles.
                               If False (default), include all events with NULL for missing articles.
        """
        con = duckdb.connect(":memory:")

        # Use INNER JOIN to only keep events with articles
        # or LEFT JOIN to keep all events even without articles (default)
        join_type = "INNER" if only_with_articles else "LEFT"

        query = f"""
            SELECT
                e.*,
                a.title as ArticleTitle,
                a.content as ArticleContent,
                a.author as ArticleAuthor,
                a.publish_date as ArticlePublishDate,
                a.content_length as ArticleContentLength,
                a.scrape_method as ArticleScrapeMethod
            FROM read_parquet('{events_parquet}') e
            {join_type} JOIN (
                SELECT DISTINCT ON (url) *
                FROM read_parquet('{articles_parquet}')
                ORDER BY url, content_length DESC
            ) a ON e.SOURCEURL = a.url
        """

        # Export directly to parquet
        con.execute(f"COPY ({query}) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")

        # Get stats
        stats = con.execute(f"""
            SELECT
                COUNT(*) as total_events,
                COUNT(ArticleTitle) as events_with_articles,
                ROUND(100.0 * COUNT(ArticleTitle) / COUNT(*), 1) as enrichment_rate
            FROM read_parquet('{output_parquet}')
        """).fetchone()

        con.close()

        if only_with_articles:
            logger.info(
                f"✅ Exported {stats[0]:,} events with successfully scraped articles (100% enrichment)"
            )
        else:
            logger.info(
                f"✅ Merged events with articles: {stats[1]:,}/{stats[0]:,} ({stats[2]}%) have article content"
            )
        logger.info(f"📁 Final output: {output_parquet}")

        return {
            "total_events": stats[0],
            "events_with_articles": stats[1],
            "enrichment_rate": stats[2],
        }

    def enrich_events_with_content(
        self,
        parquet_pattern: str = "data/parquet/events/**/*.parquet",
        limit: int = 10,
        output_file: Path | str | None = None,
        events_file: Path | str | None = None,
        final_output_file: Path | str | None = None,
    ) -> list:
        """
        Read events from parquet and enrich with article content
        Supports resume: checks for existing output and skips already-scraped URLs

        Args:
            parquet_pattern: Glob pattern for parquet files
            limit: Maximum number of articles to fetch
            output_file: Output file path for incremental saves (enables resume)
            events_file: Path to geo-enriched events file for merging
            final_output_file: Path to final merged output (geo + articles)

        Returns:
            List of enriched events
        """
        con = duckdb.connect(":memory:")

        # Check for existing attempts (both successful and failed) to skip on resume
        already_attempted = set()

        # Load successful scrapes from output file
        if output_file and Path(output_file).exists():
            try:
                existing = con.execute(
                    f"SELECT DISTINCT url FROM read_parquet('{output_file}')"
                ).fetchall()
                already_attempted.update(row[0] for row in existing)
            except Exception as e:
                logger.warning(f"Could not read existing output for resume: {e}")

        # Load failed attempts from tracking file
        failed_file = Path(output_file).parent / "failed_urls.parquet" if output_file else None
        if failed_file and failed_file.exists():
            try:
                failed = con.execute(
                    f"SELECT DISTINCT url FROM read_parquet('{failed_file}')"
                ).fetchall()
                already_attempted.update(row[0] for row in failed)
            except Exception as e:
                logger.warning(f"Could not read failed URLs for resume: {e}")

        if already_attempted:
            logger.info(
                f"🔄 Resume mode: Skipping {len(already_attempted)} already-attempted URLs (success + failed)"
            )

        # Get unique URLs from events
        # Use events_file (deduplicated/geo-enriched) if available, otherwise fall back to raw parquet
        source = events_file if events_file and Path(events_file).exists() else parquet_pattern
        query = f"""
            SELECT SOURCEURL, SQLDATE, Actor1Name, Actor1CountryCode,
                   Actor2Name, Actor2CountryCode, ActionGeo_FullName,
                   ActionGeo_CountryCode, AvgTone, EventCode
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY SOURCEURL ORDER BY SQLDATE DESC
                ) as rn
                FROM read_parquet('{source}')
                WHERE SOURCEURL IS NOT NULL AND SOURCEURL != ''
            )
            WHERE rn = 1
            ORDER BY SQLDATE DESC
            LIMIT {limit}
        """

        all_rows = con.execute(query).fetchall()

        # Filter out already-attempted URLs (both successful and failed)
        if already_attempted:
            rows = [row for row in all_rows if row[0] not in already_attempted]
            logger.info(
                f"📊 Progress: {len(already_attempted)} attempted, {len(rows)} remaining, {len(all_rows)} total"
            )
        else:
            rows = all_rows
            logger.info(f"📊 Starting fresh: {len(rows)} URLs to scrape")

        con.close()

        enriched = []
        failed_urls = []  # Track failed attempts

        # Use async event loop
        async def process_urls():
            for idx, row in enumerate(rows, 1):
                (
                    url,
                    sqldate,
                    actor1_name,
                    actor1_country,
                    actor2_name,
                    actor2_country,
                    location,
                    location_country,
                    avg_tone,
                    event_code,
                ) = row

                logger.info(f"[{idx}/{len(rows)}] Fetching: {url[:60]}...")

                article = await self.fetch_article_content(url)

                # Only append successful scrapes (excludes 403, SSL errors, timeouts, etc.)
                if article and article.get("status") == "success":
                    enriched.append(
                        {
                            "date": int(sqldate) if sqldate else None,
                            "actor1": actor1_name,
                            "actor1_country": actor1_country,
                            "actor2": actor2_name,
                            "actor2_country": actor2_country,
                            "location": location,
                            "location_country": location_country,
                            "tone": float(avg_tone) if avg_tone else None,
                            "event_code": event_code,
                            "url": url,
                            "title": article.get("title"),
                            "content": article.get("content"),
                            "author": article.get("author"),
                            "publish_date": article.get("publish_date"),
                            "content_length": article.get("length", 0),
                            "scrape_method": article.get("method"),
                        }
                    )
                else:
                    # Track failed attempts (403, SSL, timeout, all_failed, etc.)
                    failed_urls.append(
                        {
                            "url": url,
                            "status": article.get("status") if article else "unknown",
                            "error": article.get("error") if article else "no_response",
                        }
                    )

                # Incremental save every 10 attempts (successful or failed)
                total_attempts = len(enriched) + len(failed_urls)
                if output_file and total_attempts % 10 == 0:
                    # Save successful articles
                    if enriched:
                        self._save_incremental(enriched, output_file, append=True)
                        logger.info(f"💾 Progress saved: {len(enriched)} articles scraped so far")

                    # Save failed URLs for resume capability
                    if failed_urls:
                        failed_file = Path(output_file).parent / "failed_urls.parquet"
                        self._save_incremental(failed_urls, str(failed_file), append=True)
                        logger.info(
                            f"📝 Failed URLs tracked: {len(failed_urls)} attempts will be skipped on resume"
                        )

                    # Also update final merged output if events_file provided
                    if events_file and final_output_file and enriched:
                        try:
                            self.merge_articles_with_events(
                                events_file, str(output_file), str(final_output_file)
                            )
                            logger.info(f"🔗 Final merged output updated: {final_output_file}")
                        except Exception as e:
                            logger.warning(f"Could not update final merged output: {e}")

        # Run async processing
        try:
            asyncio.get_running_loop()
            logger.warning("Event loop already running - creating task instead")
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, process_urls())
                future.result()
        except RuntimeError:
            asyncio.run(process_urls())

        # Final save of any remaining articles and failed URLs
        if output_file:
            if enriched:
                self._save_incremental(enriched, output_file, append=True)
            if failed_urls:
                failed_file = Path(output_file).parent / "failed_urls.parquet"
                self._save_incremental(failed_urls, str(failed_file), append=True)
                logger.info(f"📝 Saved {len(failed_urls)} failed URLs to skip on future resumes")

        logger.info(
            f"✅ Successfully scraped {len(enriched)}/{len(rows)} articles ({len(failed_urls)} failed)"
        )
        return enriched

    def _save_incremental(self, articles: list, output_file: Path | str, append: bool = False):
        """
        Save articles incrementally (for resume capability)

        Args:
            articles: List of article dicts
            output_file: Output parquet file path
            append: If True, append to existing file (for incremental saves)
        """
        import pandas as pd

        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if not articles:
            return

        df = pd.DataFrame(articles)
        con = duckdb.connect(":memory:")

        if append and output_file.exists():
            # Append by reading existing + new data, then deduplicating by URL
            # Keep the best version per URL (longest content for articles, latest for failed URLs)
            if "content_length" in df.columns:
                # Articles: keep longest content per URL
                query = f"""
                    SELECT DISTINCT ON (url) * FROM (
                        SELECT * FROM read_parquet('{output_file}')
                        UNION ALL
                        SELECT * FROM df
                    )
                    ORDER BY url, content_length DESC
                """
            else:
                # Failed URLs: just deduplicate by URL
                query = f"""
                    SELECT DISTINCT ON (url) * FROM (
                        SELECT * FROM read_parquet('{output_file}')
                        UNION ALL
                        SELECT * FROM df
                    )
                    ORDER BY url
                """
            con.execute(f"COPY ({query}) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        else:
            # Fresh write
            relation = con.from_df(df)
            relation.to_parquet(str(output_file), compression="zstd")

        con.close()

    def save_enriched_articles(self, articles: list, output_file: Path | str):
        """Save enriched articles to Parquet file using DuckDB"""
        import pandas as pd

        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Convert list of dicts to DataFrame, then to Parquet via DuckDB
        df = pd.DataFrame(articles)

        con = duckdb.connect(":memory:")
        relation = con.from_df(df)
        relation.to_parquet(str(output_file), compression="zstd")
        con.close()

        logger.info(f"Saved {len(articles)} articles to {output_file}")

        # Also save JSON for easy inspection
        import json

        json_file = output_file.with_suffix(".json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        logger.info(f"Also saved JSON version: {json_file}")


def main():
    """CLI entry point for modern article scraping"""
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    # Parse arguments
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10

    print("=" * 70)
    print(" MODERN ARTICLE SCRAPER (Layered Hybrid 2026)")
    print("=" * 70)
    print("\nStrategy (ordered by speed):")
    print("  1. Trafilatura (fastest, F1: 0.958, no JS)")
    print("  2. Newspaper4k (fast, F1: 0.949, news-optimized)")
    print("  3. Playwright (slowest, handles JS/anti-bot)")
    print(f"\nFetching content for {limit} recent articles...\n")

    scraper = ModernArticleScraper()
    articles = scraper.enrich_events_with_content(limit=limit)

    if articles:
        output_file = Path("data/enriched_articles_modern.parquet")
        scraper.save_enriched_articles(articles, output_file)

        print(f"\n✅ Success! Enriched {len(articles)} articles")
        print(f"📄 Output: {output_file} (+ JSON version)")

        # Show method distribution
        methods = {}
        for article in articles:
            method = article.get("scrape_method", "unknown")
            methods[method] = methods.get(method, 0) + 1

        print("\n📊 Scraping Methods Used:")
        for method, count in methods.items():
            print(f"  {method}: {count} articles")

        if articles:
            print("\n📰 Sample:")
            print(f"  Date: {articles[0]['date']}")
            print(
                f"  Actors: {articles[0]['actor1']} ({articles[0]['actor1_country']}) → "
                f"{articles[0]['actor2']} ({articles[0]['actor2_country']})"
            )
            print(f"  Location: {articles[0]['location']}")
            print(f"  URL: {articles[0]['url']}")
            print(f"  Content length: {articles[0]['content_length']} chars")
            print(f"  Method: {articles[0]['scrape_method']}")
    else:
        print("\n⚠ No articles were enriched")


if __name__ == "__main__":
    main()
