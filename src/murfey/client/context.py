from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, List

import requests
import xmltodict
from pydantic import BaseModel

from murfey.client.instance_environment import (
    MovieID,
    MovieTracker,
    MurfeyID,
    MurfeyInstanceEnvironment,
)
from murfey.util.mdoc import get_global_data

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

    def post_transfer(self, transferred_file: Path, role: str = "", **kwargs):
        raise NotImplementedError(
            f"post_transfer hook must be declared in derived class to be used: {self}"
        )

    def post_first_transfer(self, transferred_file: Path, role: str = "", **kwargs):
        self.post_transfer(transferred_file, role=role, **kwargs)

    def gather_metadata(self, metadata_file: Path):
        raise NotImplementedError(
            f"gather_metadata must be declared in derived class to be used: {self}"
        )


class SPAContext(Context):
    def post_transfer(self, transferred_file: Path, role: str = "", **kwargs):
        pass


class ProcessFileIncomplete(BaseModel):
    path: Path
    image_number: int
    movie_uuid: int
    mc_uuid: int
    tag: str
    description: str = ""


class TomographyContext(Context):
    def __init__(self, acquisition_software: str):
        super().__init__(acquisition_software)
        self._tilt_series: Dict[str, List[Path]] = {}
        self._completed_tilt_series: List[str] = []
        self._last_transferred_file: Path | None = None
        self._data_collection_stash: list = []
        self._processing_job_stash: dict = {}
        self._preprocessing_triggers: dict = {}

    def _flush_data_collections(self):
        logger.debug("Flushing data collection API calls")
        for dc_data in self._data_collection_stash:
            data = {**dc_data[2], **dc_data[1].data_collection_parameters}
            requests.post(dc_data[0], json=data)
        self._data_collection_stash = []

    def _flush_processing_job(self, tag: str):
        logger.debug(f"Flushing processing job {tag}")
        if proc_data := self._processing_job_stash.get(tag):
            for pd in proc_data:
                requests.post(pd[0], json=pd[1])
            self._processing_job_stash.pop(tag)

    def _flush_preprocess(self, tag: str):
        logger.debug(f"Flushing preprocessing job {tag}")
        if tr := self._preprocessing_triggers.get(tag):
            process_file = self._complete_process_file(tr[1], tr[2])
            if process_file:
                requests.post(tr[0], json=process_file)
                self._preprocessing_triggers.pop(tag)

    def _complete_process_file(
        self,
        incomplete_process_file: ProcessFileIncomplete,
        environment: MurfeyInstanceEnvironment,
    ) -> dict:
        try:
            tag = incomplete_process_file.tag
            return {
                "path": str(incomplete_process_file.path),
                "description": incomplete_process_file.description,
                "size": incomplete_process_file.path.stat().st_size,
                "timestamp": incomplete_process_file.path.stat().st_ctime,
                "processing_job": environment._processing_jobs[tag],
                "data_collection_id": environment._data_collections[tag],
                "image_number": incomplete_process_file.image_number,
                "pixel_size": environment.data_collection_parameters[
                    "pixel_size_on_image"
                ],
                "autoproc_program_id": environment.autoproc_program_ids[tag],
                "mc_uuid": incomplete_process_file.mc_uuid,
                "movie_uuid": incomplete_process_file.movie_uuid,
            }
        except KeyError:
            return {}

    def _add_tilt(
        self,
        file_path: Path,
        extract_tilt_series: Callable[[Path], str],
        extract_tilt_angle: Callable[[Path], str],
        environment: MurfeyInstanceEnvironment | None = None,
    ) -> List[str]:
        if environment:
            environment.movies[file_path] = MovieTracker(
                movie_number=next(MovieID),
                movie_uuid=next(MurfeyID),
                motion_correction_uuid=next(MurfeyID),
            )
        try:
            tilt_series = extract_tilt_series(file_path)
            tilt_angle = extract_tilt_angle(file_path)
        except Exception:
            logger.debug(
                f"Tilt series and angle could not be determined for {file_path}"
            )
            return []
        if tilt_series in self._completed_tilt_series:
            logger.info(
                f"Tilt series {tilt_series} was previously thought complete but now {file_path} has been seen"
            )
            self._completed_tilt_series.remove(tilt_series)
        if not self._tilt_series.get(tilt_series):
            logger.info(f"New tilt series found: {tilt_series}")
            self._tilt_series[tilt_series] = [file_path]
            try:
                if environment:  # and environment._processing_jobs.get(tilt_series):
                    url = f"{str(environment.url.geturl())}/visits/{environment.visit}/start_data_collection"
                    data = {
                        "experiment_type": "tomography",
                        "tilt": tilt_series,
                        "file_extension": file_path.suffix,
                        "acquisition_software": self._acquisition_software,
                        "image_directory": str(file_path.parent),
                        "tag": tilt_series,
                    }
                    if environment.data_collection_group_id is None:
                        self._data_collection_stash.append((url, environment, data))
                    else:
                        requests.post(url, json=data)
                    proc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/register_processing_job"
                    self._processing_job_stash[tilt_series] = [
                        (proc_url, {"tag": tilt_series, "recipe": "em-tomo-preprocess"})
                    ]
                    self._processing_job_stash[tilt_series].append(
                        (proc_url, {"tag": tilt_series, "recipe": "em-tomo-align"})
                    )
                    preproc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/tomography_preprocess"
                    pfi = ProcessFileIncomplete(
                        path=file_path,
                        image_number=environment.movies[file_path].movie_number,
                        movie_uuid=environment.movies[file_path].movie_uuid,
                        mc_uuid=environment.movies[file_path].motion_correction_uuid,
                        tag=tilt_series,
                    )
                    if (
                        environment.autoproc_program_ids.get(tilt_series) is None
                        or environment.processing_job_ids.get(tilt_series) is None
                    ):
                        self._preprocessing_triggers[tilt_series] = (
                            preproc_url,
                            pfi,
                            environment,
                        )
                    else:
                        process_file = self._complete_process_file(pfi, environment)
                        requests.post(preproc_url, json=process_file)
            except Exception as e:
                logger.error(e)
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

    def _add_tomo_tilt(
        self, file_path: Path, environment: MurfeyInstanceEnvironment | None = None
    ) -> List[str]:
        return self._add_tilt(
            file_path,
            lambda x: x.name.split("_")[1],
            lambda x: x.name.split("[")[1].split("]")[0],
            environment=environment,
        )

    def _add_serialem_tilt(
        self, file_path: Path, environment: MurfeyInstanceEnvironment | None = None
    ) -> List[str]:
        delimiters = ("_", "-")
        for d in delimiters:
            if file_path.name.count(d) > 1:
                delimiter = d
                break
        else:
            delimiter = delimiters[0]

        def _extract_tilt_series(p: Path) -> str:
            split = p.name.split(delimiter)
            for s in split:
                if s.isdigit():
                    return s
            raise ValueError(
                f"No digits found in {p.name} after splitting on {delimiter}"
            )

        return self._add_tilt(
            file_path,
            _extract_tilt_series,
            lambda x: ".".join(x.name.split(delimiter)[-1].split(".")[:-1]),
            environment=environment,
        )

    def post_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ) -> List[str]:
        data_suffixes = (".mrc", ".tiff", ".tif", ".eer")
        completed_tilts = []
        if role == "detector" and transferred_file.suffix in data_suffixes:
            if self._acquisition_software == "tomo":
                completed_tilts = self._add_tomo_tilt(
                    transferred_file, environment=environment
                )
            elif self._acquisition_software == "serialem":
                completed_tilts = self._add_serialem_tilt(
                    transferred_file, environment=environment
                )
        return completed_tilts

    def post_first_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        self.post_transfer(transferred_file, role=role, environment=environment)

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
            metadata["dose_per_frame"] = None
            return metadata
        with open(metadata_file, "r") as md:
            mdoc_data = get_global_data(md)
        if not mdoc_data:
            return {}
        mdoc_metadata: dict = {}
        mdoc_metadata["experiment_type"] = "tomography"
        mdoc_metadata["voltage"] = float(mdoc_data["Voltage"])
        mdoc_metadata["image_size_x"] = int(mdoc_data["ImageSize"][0])
        mdoc_metadata["image_size_y"] = int(mdoc_data["ImageSize"][1])
        mdoc_metadata["pixel_size_on_image"] = float(mdoc_data["PixelSpacing"])
        mdoc_metadata["dose_per_frame"] = None
        return mdoc_metadata
