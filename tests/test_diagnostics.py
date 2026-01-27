"""
Test diagnostics and setup
"""

from news_cn.downloader import GDELTDownloader
from news_cn.utils.diagnostics import Diagnostics


def test_directories():
    """Test directory creation and checking"""
    results = Diagnostics.check_directories()
    assert isinstance(results, dict)
    assert len(results) > 0


def test_gdelt_availability():
    """Test GDELT file availability"""
    downloader = GDELTDownloader()
    results = Diagnostics.check_gdelt_availability(downloader)
    assert isinstance(results, dict)
    # At least one date should have data
    assert any(v.get("available", False) for v in results.values())
