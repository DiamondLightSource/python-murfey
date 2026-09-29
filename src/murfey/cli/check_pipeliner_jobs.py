"""
Validate the job numbering in a CCP-EM Pipeliner project.

SPA live processing allocates Pipeliner job numbers from several processes at
once (Murfey reserving them at schedule time, the node creator registering them
once compute finishes). When that allocation goes wrong the it shows up in
default_pipeline.star as two job types sharing a number, or as a block of
numbers unused, or jobs falling out of order.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from gemmi import cif
from pipeliner.star_keys import (
    GENERAL_BLOCK,
    JOB_COUNTER,
    PROCESS_BLOCK,
    PROCESS_PREFIX,
    PROCESS_SUFFIXES,
)

JOB_NUMBER = re.compile(r"/job(\d+)/?$")


def _job_number(process_name: str) -> int | None:
    match = JOB_NUMBER.search(process_name.rstrip("/") + "/")
    return int(match.group(1)) if match else None


def check_project(project_dir: Path) -> List[str]:
    """Return a list of problems found in the project's job numbering."""
    pipeline_file = project_dir / "default_pipeline.star"
    if not pipeline_file.is_file():
        return [f"No default_pipeline.star in {project_dir}"]

    doc = cif.read_file(str(pipeline_file))
    general = doc.find_block(GENERAL_BLOCK)
    if general is None:
        return [f"{pipeline_file} has no {GENERAL_BLOCK} block"]
    job_counter = int(general.find_value(JOB_COUNTER))

    process_block = doc.find_block(PROCESS_BLOCK)
    if process_block is None:
        return [f"{pipeline_file} has no {PROCESS_BLOCK} block"]

    # Process name and type are the first two columns of the process table.
    by_number: Dict[int, List[str]] = defaultdict(list)
    unnumbered: List[str] = []
    for row in process_block.find(PROCESS_PREFIX, PROCESS_SUFFIXES):
        name = cif.as_string(row[0])
        job_type = cif.as_string(row[2])
        number = _job_number(name)
        if number is None:
            unnumbered.append(name)
        else:
            by_number[number].append(f"{name} ({job_type})")

    problems: List[str] = []
    for name in unnumbered:
        problems.append(f"Process {name} has no jobNNN number")

    for number in sorted(n for n, procs in by_number.items() if len(procs) > 1):
        problems.append(
            f"job{number:03} is used by {len(by_number[number])} processes: "
            + ", ".join(sorted(by_number[number]))
        )

    if by_number:
        highest = max(by_number)
        gaps = sorted(set(range(1, highest + 1)) - set(by_number))
        if gaps:
            problems.append(
                "Job numbers reserved but never used: "
                + ", ".join(f"job{n:03}" for n in gaps)
            )
        if job_counter <= highest:
            problems.append(
                f"{JOB_COUNTER} is {job_counter} but job{highest:03} is already "
                "registered — the next job would reuse an existing number"
            )

    for number, procs in sorted(by_number.items()):
        for proc in procs:
            job_dir = project_dir / proc.split(" (")[0]
            if not job_dir.is_dir():
                problems.append(f"Process {proc} has no directory at {job_dir}")

    return problems


def run() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "project_dir",
        type=Path,
        nargs="?",
        default=Path.cwd(),
        help="Pipeliner project directory (default: the current directory)",
    )
    args = parser.parse_args()

    problems = check_project(args.project_dir)
    if not problems:
        print(f"{args.project_dir}: job numbering is consistent")
        return 0
    print(f"{args.project_dir}: {len(problems)} problem(s) found")
    for problem in problems:
        print(f"  - {problem}")
    return 1


if __name__ == "__main__":
    sys.exit(run())
