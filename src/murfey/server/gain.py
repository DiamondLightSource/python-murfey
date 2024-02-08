from __future__ import annotations

import asyncio
import os
from enum import Enum
from pathlib import Path
from typing import Dict, Tuple


class Camera(Enum):
    K3_FLIPX = 1
    K3_FLIPY = 2  # Talos
    FALCON = 3


def _sanitise(gain_path: Path) -> Path:
    dest = gain_path.parent / "gain" / gain_path.name.replace(" ", "_")
    dest.write_bytes(gain_path.read_bytes())
    return dest


async def prepare_gain(
    camera: int,
    gain_path: Path,
    executables: Dict[str, str],
    env: Dict[str, str],
    rescale: bool = True,
) -> Tuple[Path | None, Path | None]:
    if not all(executables.get(s) for s in ("dm2mrc", "clip", "newstack")):
        return None, None
    if camera == Camera.FALCON:
        return None, None
    if gain_path.suffix == ".dm4":
        gain_out = gain_path.parent / "gain.mrc"
        gain_out_superres = gain_path.parent / "gain_superres.mrc"
        if gain_out.is_file():
            return gain_out, gain_out_superres if rescale else gain_out
        for k, v in env.items():
            os.environ[k] = v
        (gain_path.parent / "gain").mkdir(exist_ok=True)
        gain_path = _sanitise(gain_path)
        flip = "flipx" if camera == Camera.K3_FLIPX else "flipy"
        gain_path_mrc = gain_path.with_suffix(".mrc")
        gain_path_superres = gain_path.parent / (gain_path.name + "_superres.mrc")
        dm4_proc = await asyncio.create_subprocess_shell(
            f"{executables['dm2mrc']} {gain_path} {gain_path_mrc}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await dm4_proc.communicate()
        if dm4_proc.returncode:
            return None, None
        clip_proc = await asyncio.create_subprocess_shell(
            f"{executables['clip']} {flip} {gain_path_mrc} {gain_path_superres if rescale else gain_out}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await clip_proc.communicate()
        if clip_proc.returncode:
            return None, None
        if rescale:
            newstack_proc = await asyncio.create_subprocess_shell(
                f"{executables['newstack']} -bin 2 {gain_path_superres} {gain_out}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await newstack_proc.communicate()
            if newstack_proc.returncode:
                return None, None
        if rescale:
            gain_out_superres.symlink_to(gain_path_superres)
        return gain_out, gain_out_superres if rescale else gain_out
    return None, None


async def prepare_eer_gain(
    gain_path: Path, executables: Dict[str, str], env: Dict[str, str]
) -> Tuple[Path | None, Path | None]:
    if not executables.get("tif2mrc"):
        return None, None
    gain_out = gain_path.parent / "gain.mrc"
    for k, v in env.items():
        os.environ[k] = v
    mrc_convert = await asyncio.create_subprocess_shell(
        f"{executables['tif2mrc']} {gain_path} {gain_out}"
    )
    await mrc_convert.communicate()
    if mrc_convert.returncode:
        return None, None
    return gain_out, None
