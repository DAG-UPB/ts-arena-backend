import sys
from unittest.mock import MagicMock

# Mock yaml since it's not installed but imported in plugin_loader
sys.modules['yaml'] = MagicMock()

# Also mock src.plugins.base_plugin if needed, though it might be findable if in pythonpath
# But to be safe if imports fail:
# sys.modules['src.plugins.base_plugin'] = MagicMock()

# Now import
try:
    from src.scheduler.plugin_loader import calculate_update_frequency
except ImportError:
    # Fallback if path issues, try direct import relative to script location
    import os
    sys.path.append(os.path.join(os.getcwd(), 'data-portal'))
    from src.scheduler.plugin_loader import calculate_update_frequency

test_cases = [
    ("1 minute", "3 minutes"),    # Should bump to 3 min (1/4 = 0.25 -> 3)
    ("2 minutes", "3 minutes"),   # Should bump to 3 min (2/4 = 0.5 -> 3)
    ("10 minutes", "3 minutes"),  # Should bump to 3 min (10/4 = 2.5 -> 3)
    ("15 minutes", "3 minutes"),  # Should bump to 3 min (15/4 = 3.75 -> 3 - integer division gives 3)
    ("16 minutes", "4 minutes"),  # 16/4 = 4 -> 4 minutes
    ("1 hour", "15 minutes"),     # 60/4 = 15 -> 15 minutes
    ("4 hours", "1 hour"),        # 240/4 = 60 -> 1 hour
    ("1 day", "6 hours"),         # 1440/4 = 360 -> 6 hours
]

for freq_in, expected in test_cases:
    result = calculate_update_frequency(freq_in)
    status = "PASS" if result == expected else f"FAIL (Got {result})"
    print(f"Input: {freq_in:<12} | Expected: {expected:<12} | Result: {result:<12} | {status}")
