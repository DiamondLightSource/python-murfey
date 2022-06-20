from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger("murfey.client.context")


class Context:
    def __init__(self, acquisition_software: str):
        self._acquisition_software = acquisition_software

    def post_transfer(self, transferred_file: Path):
        raise NotImplementedError(
            f"post_transfer hook must be declared in derived class to be used: {self}"
        )

    def post_first_transfer(self, transferred_file: Path):
        self.post_transfer(transferred_file)

    def gather_metadata(self):
        raise NotImplementedError(
            f"gather_metadata must be declared in derived class to be used: {self}"
        )


class SPAContext(Context):
    def post_transfer(self, transferred_file: Path):
        pass


class TomographyContext(Context):
    def __init__(self, acquisition_software: str):
        super().__init__(acquisition_software)
        self._tilt_series: Dict[str, List[Path]] = {}
        self._completed_tilt_series: List[str] = []
        self._last_transferred_file: Path | None = None

    def _add_tomo_tilt(self, file_path: Path) -> List[str]:
        tilt_series = file_path.name.split("_")[1]
        tilt_angle = file_path.name.split("[")[1].split("]")[0]
        if tilt_series in self._completed_tilt_series:
            logger.info(
                f"Tilt series {tilt_series} was previously thought complete but now {file_path} has been seen"
            )
            self._completed_tilt_series.remove(tilt_series)
        if not self._tilt_series.get(tilt_series):
            self._tilt_series[tilt_series] = [file_path]
        else:
            self._tilt_series[tilt_series].append(file_path)
        if self._last_transferred_file:
            last_tilt_series = self._last_transferred_file.name.split("_")[1]
            last_tilt_angle = self._last_transferred_file.name.split("[")[1].split("]")[
                0
            ]
            if last_tilt_series != tilt_series and last_tilt_angle != tilt_angle:
                newly_completed_series = []
                tilt_series_size = max(len(ts) for ts in self._tilt_series.values())
                this_tilt_series_size = len(self._tilt_series[tilt_series])
                if this_tilt_series_size >= tilt_series_size:
                    self._completed_tilt_series.append(tilt_series)
                    self._last_transferred_file = file_path
                    newly_completed_series.append(tilt_series)
                for ts, ta in self._tilt_series.items():
                    if (
                        len(ta) >= tilt_series_size
                        and ts not in self._completed_tilt_series
                    ):
                        newly_completed_series.append(ts)
                        self._completed_tilt_series.append(ts)
                logger.info(
                    f"The following tilt series are considered complete: {newly_completed_series}"
                )
                return newly_completed_series
        self._last_transferred_file = file_path
        return []

    def post_transfer(self, transferred_file: Path) -> List[str]:
        completed_tilts = []
        if self._acquisition_software == "tomo":
            completed_tilts = self._add_tomo_tilt(transferred_file)
        return completed_tilts
