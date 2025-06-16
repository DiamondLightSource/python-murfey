from __future__ import annotations

import asyncio
import logging
import os
from enum import Enum
from pathlib import Path
from typing import Dict, Tuple

from murfey.util import secure_path

logger = logging.getLogger("murfey.server.gain")


class Camera(Enum):
    K3_FLIPX = 1
    K3_FLIPY = 2  # Talos
    FALCON = 3


def _sanitise(gain_path: Path, tag: str) -> Path:
    if tag:
        dest = gain_path.parent / f"gain_{tag}" / gain_path.name.replace(" ", "_")
    else:
        dest = gain_path.parent / "gain" / gain_path.name.replace(" ", "_")
    dest.write_bytes(gain_path.read_bytes())
    return dest


async def prepare_gain(
    camera: int,
    gain_path: Path,
    executables: Dict[str, str],
    env: Dict[str, str],
    rescale: bool = True,
    tag: str = "",
) -> Tuple[Path | None, Path | None]:
    if not all(executables.get(s) for s in ("dm2mrc", "clip", "newstack")):
        logger.error("No executables were provided to prepare the gain reference with")
        return None, None
    if camera == Camera.FALCON:
        logger.info("Gain reference preparation not needed for Falcon detector")
        return None, None
    if gain_path.suffix == ".dm4":
        gain_out = (
            gain_path.parent / f"gain_{tag}.mrc"
            if tag
            else gain_path.parent / "gain.mrc"
        )
        gain_out_superres = (
            gain_path.parent / f"gain_{tag}_superres.mrc"
            if tag
            else gain_path.parent / "gain_superres.mrc"
        )
        if secure_path(gain_out).is_file():
            return gain_out, gain_out_superres if rescale else gain_out
        for k, v in env.items():
            os.environ[k] = v
        if tag:
            secure_path(gain_path.parent / f"gain_{tag}").mkdir(exist_ok=True)
        else:
            secure_path(gain_path.parent / "gain").mkdir(exist_ok=True)
        gain_path = _sanitise(gain_path, tag)
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
            logger.error(
                "Error encountered while trying to process the gain reference with 'dm2mrc': \n"
                f"{stderr.decode('utf-8').strip()}"
            )
            return None, None
        clip_proc = await asyncio.create_subprocess_shell(
            f"{executables['clip']} {flip} {secure_path(gain_path_mrc)} {secure_path(gain_path_superres) if rescale else secure_path(gain_out)}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await clip_proc.communicate()
        if clip_proc.returncode:
            logger.error(
                "Error encountered while trying to process the gain reference with 'clip': \n"
                f"{stderr.decode('utf-8').strip()}"
            )
            return None, None
        if rescale:
            newstack_proc = await asyncio.create_subprocess_shell(
                f"{executables['newstack']} -bin 2 {secure_path(gain_path_superres)} {secure_path(gain_out)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await newstack_proc.communicate()
            if newstack_proc.returncode:
                logger.error(
                    "Error encountered while trying to process the gain reference with 'newstack': \n"
                    f"{stderr.decode('utf-8').strip()}"
                )
                return None, None
        if rescale:
            secure_path(gain_out_superres).symlink_to(secure_path(gain_path_superres))
        return gain_out, gain_out_superres if rescale else gain_out
    return None, None


async def prepare_eer_gain(
    gain_path: Path, executables: Dict[str, str], env: Dict[str, str], tag: str = ""
) -> Tuple[Path | None, Path | None]:
    if not executables.get("tif2mrc"):
        logger.error(
            "No executables were provided to prepare the EER gain reference with"
        )
        return None, None
    gain_out = (
        gain_path.parent / f"gain_{tag}.mrc" if tag else gain_path.parent / "gain.mrc"
    )
    for k, v in env.items():
        os.environ[k] = v
    mrc_convert = await asyncio.create_subprocess_shell(
        f"{executables['tif2mrc']} {secure_path(gain_path)} {secure_path(gain_out)}"
    )
    stdout, stderr = await mrc_convert.communicate()
    if mrc_convert.returncode:
        logger.error(
            "Error encountered while trying to process the EER gain reference: \n"
            f"{stderr.decode('utf-8').strip()}"
        )
        return None, None
    return gain_out, None
