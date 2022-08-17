from __future__ import annotations

from typing import TextIO


def get_block(mdocfile: TextIO) -> dict:
    while line := mdocfile.readline():
        if line.startswith("[ZValue"):
            break
    else:
        return {}
    as_dict = {}
    while line := mdocfile.readline():
        if line.replace(" ", "") == "\n":
            break
        kv = [p.strip() for p in line.split("=")]
        as_dict[kv[0]] = tuple(kv[1].split()) if " " in kv[1] else kv[1]
    return as_dict
