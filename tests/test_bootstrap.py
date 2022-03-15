from __future__ import annotations

import subprocess
import sys


def test_murfey_bootstrap_module_is_available():
    python = sys.executable
    result = subprocess.run([python, "-m", "murfey.bootstrap", "--help"])
    assert result.returncode == 0
