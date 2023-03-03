from __future__ import annotations

from datetime import datetime
from typing import TextIO


def _basic_parse(line: str) -> dict:
    kv = [p.strip() for p in line.split("=")]
    if kv[0] == "DateTime":
        return {kv[0]: datetime.strptime(kv[1], "%d-%b-%Y %H:%M:%S")}
    return {kv[0]: tuple(kv[1].split()) if " " in kv[1] else kv[1]}


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
        as_dict.update(_basic_parse(line))
    return as_dict


def get_num_blocks(mdocfile: TextIO) -> int:
    num_blocks = 0
    while line := mdocfile.readline():
        if line.startswith("[ZValue"):
            num_blocks += 1
    return num_blocks


def get_global_data(mdocfile: TextIO) -> dict:
    as_dict = {}
    while line := mdocfile.readline():
        if line.startswith("[") or line.replace(" ", "") == "\n":
            break
        as_dict.update(_basic_parse(line))
    return as_dict
