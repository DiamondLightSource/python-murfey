from __future__ import annotations

from pathlib import Path


def determine_gain_ref(gain_ref_dir: Path) -> Path:
    candidates = list(gain_ref_dir.glob("*"))
    candidates = sorted(candidates, key=lambda x: x.stat().st_mtime, reverse=True)
    viable_candidates = candidates[:3]
    viable_candidates = sorted(
        viable_candidates, key=lambda x: x.stat().st_size, reverse=True
    )
    if viable_candidates:
        return viable_candidates[0]
    raise RuntimeError(f"Cannot identify a gain reference candidate in {gain_ref_dir}")
