"""
Pipeline utility tools for managing state and data
"""

import logging

from ..consolidator import DataConsolidator
from ..state_manager import StateManager

logger = logging.getLogger(__name__)


def show_stats():
    """Show pipeline statistics"""
    state = StateManager()
    consolidator = DataConsolidator()

    print("=" * 70)
    print(" PIPELINE STATISTICS")
    print("=" * 70)

    # State stats
    stats = state.get_stats()
    print("\n📊 Processing Stats:")
    print(f"   Total files processed: {stats['total_files_processed']}")
    print(f"   Successful: {stats['successful']}")
    print(f"   Failed: {stats['failed']}")
    print(f"   Last run: {stats['last_run'] or 'Never'}")

    # Consolidation stats
    print("\n📦 Consolidation Stats:")
    for data_type in ["events", "mentions", "gkg"]:
        cons_stats = consolidator.get_stats(data_type)
        if cons_stats["total_days"] > 0:
            print(f"   {data_type.capitalize()}:")
            print(f"     Total days: {cons_stats['total_days']}")
            print(f"     Consolidated: {cons_stats['consolidated_days']}")
            print(f"     Pending: {cons_stats['unconsolidated_days']}")

    print("\n" + "=" * 70)


def reset_state():
    """Reset pipeline state (use with caution!)"""
    response = input("⚠️  This will reset all processing state. Continue? (yes/no): ")
    if response.lower() == "yes":
        state = StateManager()
        state.reset()
        print("✓ State reset complete")
    else:
        print("Cancelled")


def consolidate_all():
    """Manually trigger consolidation of all pending days"""
    consolidator = DataConsolidator()
    state = StateManager()

    print("=" * 70)
    print(" MANUAL CONSOLIDATION")
    print("=" * 70)

    for data_type in ["events", "mentions", "gkg"]:
        print(f"\n📦 Consolidating {data_type}...")
        dates = consolidator.consolidate_all_pending_days(data_type)

        for date_str in dates:
            state.mark_day_consolidated(date_str)
            print(f"  ✓ {date_str}")

        if not dates:
            print("  No pending days to consolidate")

    print("\n✓ Consolidation complete!")


def main():
    """CLI entry point for pipeline tools"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: news-cn-tools [stats|reset|consolidate]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "stats":
        show_stats()
    elif command == "reset":
        reset_state()
    elif command == "consolidate":
        consolidate_all()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
