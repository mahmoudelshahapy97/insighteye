"""Deselects the test IDs listed in tests/known_failures.txt (used by CI).

Unlike --deselect, which matches node ID prefixes, this matches IDs exactly, so
listing test_get_user_cameras doesn't also skip test_get_user_cameras_empty.
Load with: PYTHONPATH=tests pytest -p known_failures_plugin
"""
from pathlib import Path

KNOWN_FAILURES = Path(__file__).with_name("known_failures.txt")


def pytest_collection_modifyitems(config, items):
    known = {
        line.strip()
        for line in KNOWN_FAILURES.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    keep = [item for item in items if item.nodeid not in known]
    dropped = [item for item in items if item.nodeid in known]
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = keep
