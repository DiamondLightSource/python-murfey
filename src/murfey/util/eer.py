from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO, Literal

from murfey.util import secure_path


def _count_ifds(file_stream: BinaryIO) -> int:
    file_stream.seek(0, 0)
    # get the byte order mark
    # for TIFFs II means intel ordering (little-endian) and MM means motorola ordering (big-endian)
    bom = bytes(file_stream.read(2)).decode("utf-8")
    byteorder: Literal["little", "big"] = "little" if bom == "II" else "big"
    file_stream.seek(8, 0)
    ifd = int.from_bytes(file_stream.read(8), byteorder)
    n = 0
    # bounce between IFDs and count them until they run out
    # EER mostly follows the BigTIFF file format (https://www.awaresystems.be/imaging/tiff/bigtiff.html)
    while ifd != 0:
        n += 1
        file_stream.seek(ifd, 0)
        num_tags = int.from_bytes(file_stream.read(8), byteorder)
        file_stream.seek(ifd + 8 + 20 * num_tags, 0)
        ifd = int.from_bytes(file_stream.read(8), byteorder)
    return n


def num_frames(eer_path: os.PathLike) -> int:
    with open(secure_path(Path(eer_path)), "rb") as eer:
        n = _count_ifds(eer)
    return n
