"""Make `src` importable when pytest is invoked from the data-portal directory.

pytest inserts the test file's own directory on sys.path (no __init__.py here), not the
service root, so the plugin package would not resolve without this.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
