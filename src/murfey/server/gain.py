from __future__ import annotations

import asyncio
from enum import Enum
from pathlib import Path
from typing import Dict


class Camera(Enum):
    K3_FLIPX = 1
    K3_FLIPY = 2  # Talos
    FALCON = 3


async def prepare_gain(
    camera: int, gain_path: Path, executables: Dict[str, str]
) -> Path | None:
    if not all(executables.get(s) for s in ("dm2mrc", "clip", "newstack")):
        return None
    if camera == Camera.FALCON:
        return None
    if gain_path.suffix == ".dm4":
        flip = "flipx" if camera == Camera.K3_FLIPX else "flipy"
        gain_path_mrc = gain_path.with_suffix(".mrc")
        gain_path_superres = gain_path.parent / (gain_path.name + "_superres.mrc")
        gain_path_stdres = gain_path.parent / (gain_path.name + "_stdres.mrc")
        dm4_proc = await asyncio.create_subprocess_shell(
            f"{executables['dm2mrc']} {gain_path} {gain_path_mrc}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await dm4_proc.communicate()
        if dm4_proc.returncode:
            return None
        clip_proc = await asyncio.create_subprocess_shell(
            f"{executables['clip']} {flip} {gain_path_mrc} {gain_path_superres}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await clip_proc.communicate()
        if clip_proc.returncode:
            return None
        newstack_proc = await asyncio.create_subprocess_shell(
            f"{executables['newstack']} {flip} {gain_path_superres} {gain_path_stdres}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await newstack_proc.communicate()
        if newstack_proc.returncode:
            return None
        return gain_path_stdres
    return None
