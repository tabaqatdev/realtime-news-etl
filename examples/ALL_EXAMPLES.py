"""
Complete Examples Collection for news-cn
All usage examples in one file

Run specific example:
    python examples/ALL_EXAMPLES.py simple
    python examples/ALL_EXAMPLES.py fluent
    python examples/ALL_EXAMPLES.py query
    python examples/ALL_EXAMPLES.py all
"""

import sys
from pathlib import Path

# Ensure news_cn is importable
sys.path.insert(0, str(Path(__file__).parent.parent))


# ============================================================================
# EXAMPLE 1: Simple One-Line Usage
# ============================================================================
def example_simple():
    """Simplest possible usage - one line"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 1: Simple One-Line Usage")
    print("=" * 70 + "\n")

    from news_cn import collect_news

    # Collect all Saudi news from Jan 1, 2026
    print("Collecting Saudi Arabia news...")
    results = collect_news()

    print(f"\n✅ Collected {len(results)} days of news")
    for date, path in list(results.items())[:3]:
        print(f"  {date}: {path}")


# ============================================================================
# EXAMPLE 2: Fluent API
# ============================================================================
def example_fluent():
    """Using the fluent API builder"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 2: Fluent API Builder")
    print("=" * 70 + "\n")

    from news_cn import SimplePipeline

    # Build pipeline with chaining
    pipeline = (
        SimplePipeline()
        .for_country("SA")
        .from_date("2026-01-01")
        .use_batch_processing()
        .output_to("data/parquet")
        .run()
    )

    print(f"\n✅ Processed {len(pipeline.results)} days")

    # Query the results
    print("\nMost recent events:")
    events = pipeline.query(limit=5)
    for i, event in enumerate(events, 1):
        print(f"\n{i}. {event['SQLDATE']}")
        print(f"   Actors: {event['Actor1Name']} → {event['Actor2Name']}")
        print(f"   Location: {event['Location']}")
        print(f"   Tone: {event['AvgTone']:.2f}")


# ============================================================================
# EXAMPLE 3: Quick Query Existing Data
# ============================================================================
def example_query():
    """Query already collected data"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 3: Query Existing Data")
    print("=" * 70 + "\n")

    from news_cn import query_news

    # Quick query of recent events
    events = query_news(country="SA", limit=10)

    if events:
        print(f"Found {len(events)} recent events:\n")
        for i, event in enumerate(events, 1):
            print(f"{i}. {event['SQLDATE']}: {event['Actor1Name']} → {event['Actor2Name']}")
            print(f"   Location: {event['Location']}")
            print(f"   URL: {event['URL'][:60]}...")
            print()
    else:
        print("⚠ No data found. Run collect_news() first.")


# ============================================================================
# EXAMPLE 4: Custom Configuration
# ============================================================================
def example_custom_config():
    """Advanced usage with custom configuration"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 4: Custom Configuration")
    print("=" * 70 + "\n")

    from news_cn import Config, GDELTDownloader

    # Custom configuration
    config = Config()
    config.DUCKDB_MEMORY_LIMIT = "8GB"
    config.DOWNLOAD_WORKERS = 20
    config.TARGET_COUNTRY_CODE = "AE"  # UAE

    print("Configuration:")
    print(f"  Memory: {config.DUCKDB_MEMORY_LIMIT}")
    print(f"  Workers: {config.DOWNLOAD_WORKERS}")
    print(f"  Country: {config.TARGET_COUNTRY_CODE}")

    # Initialize components
    downloader = GDELTDownloader(config=config)
    # processor = GDELTProcessor(strategy="batch", config=config)

    # Get files
    file_list = downloader.get_available_files(start_date=config.START_DATE, data_types=["export"])

    print(f"\nFound {len(file_list)} files to process")

    # Process (commented out to avoid actual download)
    # results = processor.process_all_days(file_list)
    # print(f"\n✅ Processed {len(results)} days")


# ============================================================================
# EXAMPLE 5: Multi-Country Collection
# ============================================================================
def example_multi_country():
    """Collect data for multiple countries"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 5: Multi-Country Collection")
    print("=" * 70 + "\n")

    from news_cn import collect_news

    countries = {
        "SA": "Saudi Arabia",
        "AE": "UAE",
        "QA": "Qatar",
        "KW": "Kuwait",
    }

    print("Collecting news for GCC countries...")

    for code, name in countries.items():
        print(f"\n{name} ({code}):")
        try:
            results = collect_news(country=code, start_date="2026-01-01", output_dir=f"data/{code}")
            print(f"  ✓ {len(results)} days collected")
        except Exception as e:
            print(f"  ✗ Error: {e}")


# ============================================================================
# EXAMPLE 6: Data Analysis with DuckDB
# ============================================================================
def example_analysis():
    """Analyze collected data with DuckDB"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 6: Data Analysis with DuckDB")
    print("=" * 70 + "\n")

    import duckdb

    conn = duckdb.connect()

    try:
        # Count events
        total = conn.execute("SELECT COUNT(*) FROM 'data/parquet/events/**/*.parquet'").fetchone()[
            0
        ]
        print(f"Total events: {total:,}")

        # Date range
        date_range = conn.execute(
            """
            SELECT MIN(SQLDATE) as earliest, MAX(SQLDATE) as latest
            FROM 'data/parquet/events/**/*.parquet'
        """
        ).fetchone()
        print(f"Date range: {date_range[0]} to {date_range[1]}")

        # Top actors
        print("\nTop 10 most mentioned actors:")
        top_actors = conn.execute(
            """
            SELECT Actor1Name, COUNT(*) as count
            FROM 'data/parquet/events/**/*.parquet'
            WHERE Actor1Name IS NOT NULL
            GROUP BY Actor1Name
            ORDER BY count DESC
            LIMIT 10
        """
        ).fetchall()

        for i, (actor, count) in enumerate(top_actors, 1):
            print(f"  {i}. {actor}: {count} mentions")

        # Sentiment analysis
        avg_tone = conn.execute(
            """
            SELECT AVG(AvgTone) as avg_sentiment
            FROM 'data/parquet/events/**/*.parquet'
        """
        ).fetchone()[0]
        print(f"\nAverage sentiment: {avg_tone:.2f}")

    except Exception as e:
        print(f"⚠ Error: {e}")
        print("\nRun collect_news() first to generate data.")

    conn.close()


# ============================================================================
# EXAMPLE 7: API Clients (Alternative Data Source)
# ============================================================================
def example_api_clients():
    """Using GDELT API clients for recent data"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 7: GDELT API Clients")
    print("=" * 70 + "\n")

    from news_cn import GDELTAPIClient

    # Initialize API client (free, no keys needed)
    api = GDELTAPIClient()

    print("Fetching recent news from API...")
    articles = api.get_recent_saudi_news(days_back=7)

    if articles:
        print(f"✓ Retrieved {len(articles)} articles\n")
        print("Sample articles:")
        for i, article in enumerate(articles[:3], 1):
            print(f"\n{i}. {article.get('title', 'No title')}")
            print(f"   URL: {article.get('url', 'N/A')}")
            print(f"   Date: {article.get('seendate', 'N/A')}")
    else:
        print("⚠ No articles found")


# ============================================================================
# EXAMPLE 8: Convenience Shortcuts
# ============================================================================
def example_shortcuts():
    """Using convenience shortcut functions"""
    print("\n" + "=" * 70)
    print(" EXAMPLE 8: Convenience Shortcuts")
    print("=" * 70 + "\n")

    from news_cn import collect_saudi_news, collect_uae_news

    # Quick shortcuts for common countries
    print("Collecting last 7 days of Saudi news...")
    saudi_results = collect_saudi_news(days_back=7)
    print(f"✓ Saudi Arabia: {len(saudi_results)} days")

    print("\nCollecting last 7 days of UAE news...")
    uae_results = collect_uae_news(days_back=7)
    print(f"✓ UAE: {len(uae_results)} days")


# ============================================================================
# Main Runner
# ============================================================================
def main():
    """Run examples based on command line argument"""
    examples = {
        "simple": example_simple,
        "fluent": example_fluent,
        "query": example_query,
        "config": example_custom_config,
        "multi": example_multi_country,
        "analysis": example_analysis,
        "api": example_api_clients,
        "shortcuts": example_shortcuts,
    }

    if len(sys.argv) < 2:
        print("\n📚 Available Examples:")
        print("=" * 70)
        print("\nUsage: python examples/ALL_EXAMPLES.py <example_name>\n")
        print("Examples:")
        print("  simple      - One-line usage (easiest)")
        print("  fluent      - Fluent API builder")
        print("  query       - Query existing data")
        print("  config      - Custom configuration")
        print("  multi       - Multi-country collection")
        print("  analysis    - Data analysis with DuckDB")
        print("  api         - Using API clients")
        print("  shortcuts   - Convenience functions")
        print("  all         - Run all examples")
        print("\nExample:")
        print("  python examples/ALL_EXAMPLES.py simple")
        return

    example_name = sys.argv[1].lower()

    if example_name == "all":
        print("\n🚀 Running all examples...")
        for name, func in examples.items():
            try:
                func()
            except Exception as e:
                print(f"\n⚠ Error in {name}: {e}")
    elif example_name in examples:
        examples[example_name]()
    else:
        print(f"⚠ Unknown example: {example_name}")
        print(f"Available: {', '.join(examples.keys())}, all")


if __name__ == "__main__":
    main()
