from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, List

import mdocfile
import xmltodict

logger = logging.getLogger("murfey.client.context")


def detect_acquisition_software(dir_for_transfer: Path) -> str:
    glob = dir_for_transfer.glob("*")
    for f in glob:
        if f.name.startswith("EPU") or f.name.startswith("GridSquare"):
            return "epu"
        if f.name.startswith("Position") or f.suffix == ".mdoc":
            return "tomo"
    return ""


class Context:
    def __init__(self, acquisition_software: str):
        self._acquisition_software = acquisition_software

    def post_transfer(self, transferred_file: Path, role: str = ""):
        raise NotImplementedError(
            f"post_transfer hook must be declared in derived class to be used: {self}"
        )

    def post_first_transfer(self, transferred_file: Path, role: str = ""):
        self.post_transfer(transferred_file, role=role)

    def gather_metadata(self, metadata_file: Path):
        raise NotImplementedError(
            f"gather_metadata must be declared in derived class to be used: {self}"
        )


class SPAContext(Context):
    def post_transfer(self, transferred_file: Path, role: str = ""):
        pass


class TomographyContext(Context):
    def __init__(self, acquisition_software: str):
        super().__init__(acquisition_software)
        self._tilt_series: Dict[str, List[Path]] = {}
        self._completed_tilt_series: List[str] = []
        self._last_transferred_file: Path | None = None

    def _add_tilt(
        self,
        file_path: Path,
        extract_tilt_series: Callable[[Path], str],
        extract_tilt_angle: Callable[[Path], str],
    ) -> List[str]:
        tilt_series = extract_tilt_series(file_path)
        tilt_angle = extract_tilt_angle(file_path)
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
            last_tilt_series = extract_tilt_series(self._last_transferred_file)
            last_tilt_angle = extract_tilt_angle(self._last_transferred_file)
            self._last_transferred_file = file_path
            if last_tilt_series != tilt_series and last_tilt_angle != tilt_angle:
                newly_completed_series = []
                if self._tilt_series:
                    tilt_series_size = max(len(ts) for ts in self._tilt_series.values())
                else:
                    tilt_series_size = 0
                this_tilt_series_size = len(self._tilt_series[tilt_series])
                if this_tilt_series_size >= tilt_series_size:
                    self._completed_tilt_series.append(tilt_series)
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

    def _add_tomo_tilt(self, file_path: Path) -> List[str]:
        return self._add_tilt(
            file_path,
            lambda x: x.name.split("_")[1],
            lambda x: x.name.split("[")[1].split("]")[0],
        )

    # def _add_serialem_tilt(self, file_path: Path) -> List[str]:

    def post_transfer(self, transferred_file: Path, role: str = "") -> List[str]:
        completed_tilts = []
        if self._acquisition_software == "tomo":
            if role == "detector":
                completed_tilts = self._add_tomo_tilt(transferred_file)
        return completed_tilts

    def gather_metadata(self, metadata_file: Path) -> dict:
        if metadata_file.suffix not in (".mdoc", ".xml"):
            raise ValueError(
                f"Tomography gather_metadata method expected xml or mdoc file not {metadata_file.name}"
            )
        if not metadata_file.is_file():
            logger.debug(f"Metadata file {metadata_file} not found")
            return {}
        if metadata_file.suffix == ".xml":
            with open(metadata_file, "r") as xml:
                for_parsing = xml.read()
                data = xmltodict.parse(for_parsing)
            metadata: dict = {}
            metadata["experiment_type"] = "tomography"
            metadata["voltage"] = 300
            metadata["image_size_x"] = data["Acquisition"]["Info"]["ImageSize"]["Width"]
            metadata["image_size_y"] = data["Acquisition"]["Info"]["ImageSize"][
                "Height"
            ]
            metadata["pixel_size_on_image"] = float(
                data["Acquisition"]["Info"]["SensorPixelSize"]["Height"]
            )
            return metadata
        mdoc_data = mdocfile.read(metadata_file)
        mdoc_metadata: dict = {}
        mdoc_metadata["experiment_type"] = "tomography"
        mdoc_metadata["voltage"] = mdoc_data.iloc[0].voltage
        mdoc_metadata["image_size_x"] = mdoc_data.iloc[0].image_size[0]
        mdoc_metadata["image_size_y"] = mdoc_data.iloc[0].image_size[1]
        mdoc_metadata["pixel_size_on_image"] = float(mdoc_data.iloc[0].pixel_spacing)
        return mdoc_metadata
