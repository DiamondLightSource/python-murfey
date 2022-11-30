from __future__ import annotations

import logging
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, List, Optional, OrderedDict

import requests
import xmltodict
from pydantic import BaseModel

from murfey.client.contexts.tomo import tomo_tilt_info
from murfey.client.instance_environment import (
    MovieID,
    MovieTracker,
    MurfeyID,
    MurfeyInstanceEnvironment,
    global_env_lock,
)
from murfey.client.tui.forms import TUIFormValue
from murfey.util.mdoc import get_global_data

# import time

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
    dest: Path
    source: Path
    image_number: int
    mc_uuid: int
    tag: str
    description: str = ""


class TomographyContext(Context):
    def __init__(self, acquisition_software: str):
        super().__init__(acquisition_software)
        self._tilt_series: Dict[str, List[Path]] = {}
        self._completed_tilt_series: List[str] = []
        self._aligned_tilt_series: List[str] = []
        self._motion_corrected_tilt_series: Dict[str, List[Path]] = {}
        self._last_transferred_file: Path | None = None
        self._data_collection_stash: list = []
        self._processing_job_stash: dict = {}
        self._preprocessing_triggers: dict = {}
        self._lock: RLock = RLock()
        self._extract_tilt_series: Callable[[Path], str] | None = None
        self._extract_tilt_tag: Callable[[Path], str] | None = None

    def _flush_data_collections(self):
        logger.info("Flushing data collection API calls")
        for dc_data in self._data_collection_stash:
            data = {**dc_data[2], **dc_data[1].data_collection_parameters}
            requests.post(dc_data[0], json=data)
        self._data_collection_stash = []

    def _flush_processing_job(self, tag: str):
        # logger.info(
        #     f"Flushing processing job {tag}, {self._processing_job_stash.get(tag)}"
        # )
        if proc_data := self._processing_job_stash.get(tag):
            for pd in proc_data:
                requests.post(pd[0], json=pd[1])
            self._processing_job_stash.pop(tag)

    def _flush_preprocess(self, tag: str, app_id: int):
        # logger.info(f"Flushing preprocessing requests {tag}")
        if tag_tr := self._preprocessing_triggers.get(tag):
            for tr in tag_tr:
                process_file = self._complete_process_file(tr[1], tr[2], app_id)
                if process_file:
                    requests.post(tr[0], json=process_file)
            self._preprocessing_triggers.pop(tag)

    def _check_for_alignment(
        self,
        movie_path: Path,
        motion_corrected_path: Path,
        url: str,
        dcid: int,
        pjid: int,
        appid: int,
        mvid: int,
        tilt_angles: List,
        tilt_offset: Optional[float],
    ):
        if self._extract_tilt_series and self._extract_tilt_tag:
            tilt_series = (
                f"{self._extract_tilt_tag(movie_path)}_{self._extract_tilt_series(movie_path)}"
                if self._extract_tilt_tag(movie_path)
                else self._extract_tilt_series(movie_path)
            )
        else:
            return

        if self._motion_corrected_tilt_series.get(
            tilt_series
        ) and motion_corrected_path not in self._motion_corrected_tilt_series.get(
            tilt_series, {}
        ):
            self._motion_corrected_tilt_series[tilt_series].append(
                motion_corrected_path
            )
        else:
            self._motion_corrected_tilt_series[tilt_series] = [motion_corrected_path]
        if tilt_series in self._completed_tilt_series:
            if (
                len(self._motion_corrected_tilt_series[tilt_series])
                == len(self._tilt_series[tilt_series])
                and len(self._motion_corrected_tilt_series[tilt_series]) > 1
                and tilt_series not in self._aligned_tilt_series
            ):
                try:

                    series_data: dict = {
                        "name": tilt_series,
                        "file_tilt_list": str(tilt_angles),
                        "dcid": dcid,
                        "processing_job": pjid,
                        "autoproc_program_id": appid,
                        "motion_corrected_path": str(motion_corrected_path),
                        "movie_id": mvid,
                        "tilt_offset": tilt_offset,
                    }
                    requests.post(url, json=series_data)
                    with self._lock:
                        self._aligned_tilt_series.append(tilt_series)
                except Exception as e:
                    logger.warning(f"Data error {e}")

    def _complete_process_file(
        self,
        incomplete_process_file: ProcessFileIncomplete,
        environment: MurfeyInstanceEnvironment,
        app_id: int,
    ) -> dict:
        try:
            with global_env_lock:
                tag = incomplete_process_file.tag

                new_dict = {
                    "path": str(incomplete_process_file.dest),
                    "description": incomplete_process_file.description,
                    "size": incomplete_process_file.source.stat().st_size,
                    "timestamp": incomplete_process_file.source.stat().st_ctime,
                    "processing_job": environment.processing_job_ids[tag][
                        "em-tomo-preprocess"
                    ],
                    "data_collection_id": environment.data_collection_ids[tag],
                    "image_number": incomplete_process_file.image_number,
                    "pixel_size": environment.data_collection_parameters[
                        "pixel_size_on_image"
                    ],
                    "autoproc_program_id": app_id,
                    "mc_uuid": incomplete_process_file.mc_uuid,
                    "dose_per_frame": environment.data_collection_parameters.get(
                        "dose_per_frame"
                    ),
                    "mc_binning": environment.data_collection_parameters.get(
                        "motion_corr_binning", 1
                    ),
                    "gain_ref": environment.data_collection_parameters.get("gain_ref"),
                }
                return new_dict
        except KeyError:
            logger.warning("Key error encountered in _complete_process_file")
            return {}

    def _add_tilt(
        self,
        file_path: Path,
        extract_tilt_series: Callable[[Path], str],
        extract_tilt_angle: Callable[[Path], str],
        extract_tilt_tag: Callable[[Path], str],
        environment: MurfeyInstanceEnvironment | None = None,
        required_position_files: List[Path] | None = None,
    ) -> List[str]:
        # required_position_files = required_position_files or []
        if not self._extract_tilt_series:
            self._extract_tilt_series = extract_tilt_series
        if not self._extract_tilt_tag:
            self._extract_tilt_tag = extract_tilt_tag
        try:
            tilt_series_num = extract_tilt_series(file_path)
            tilt_angle = extract_tilt_angle(file_path)
            tilt_tag = extract_tilt_tag(file_path)
            try:
                float(tilt_series_num)
                float(tilt_angle)
            except ValueError:
                return []
            tilt_series = (
                f"{tilt_tag}_{tilt_series_num}" if tilt_tag else tilt_series_num
            )

        except Exception:
            logger.debug(
                f"Tilt series and angle could not be determined for {file_path}"
            )
            return []

        if environment:
            machine_config = (
                {}
                if environment.demo
                else requests.get(f"{str(environment.url.geturl())}/machine/").json()
            )
            if environment.visit in environment.default_destination:
                file_transferred_to = (
                    Path(machine_config.get("rsync_basepath", ""))
                    / Path(environment.default_destination)
                    / file_path.name
                )
            else:
                file_transferred_to = (
                    Path(machine_config.get("rsync_basepath", ""))
                    / Path(environment.default_destination)
                    / environment.visit
                    / file_path.name
                )
            environment.movies[file_transferred_to] = MovieTracker(
                movie_number=next(MovieID),
                motion_correction_uuid=next(MurfeyID),
            )
            environment.movie_tilt_pair[file_transferred_to] = tilt_series
            if environment.tilt_angles.get(tilt_series):
                environment.tilt_angles[tilt_series].append(
                    [str(file_transferred_to), tilt_angle]
                )
            else:
                environment.tilt_angles[tilt_series] = [
                    [str(file_transferred_to), tilt_angle]
                ]
        if tilt_series in self._completed_tilt_series:
            logger.info(
                f"Tilt series {tilt_series} was previously thought complete but now {file_path} has been seen"
            )
            self._completed_tilt_series.remove(tilt_series)
            if tilt_series in self._aligned_tilt_series:
                with self._lock:
                    self._aligned_tilt_series.remove(tilt_series)

        if not self._tilt_series.get(tilt_series):
            logger.info(f"New tilt series found: {tilt_series}")
            self._tilt_series[tilt_series] = [file_path]
            try:
                if environment:
                    url = f"{str(environment.url.geturl())}/visits/{environment.visit}/start_data_collection"
                    data = {
                        "experiment_type": "tomography",
                        "file_extension": file_path.suffix,
                        "acquisition_software": self._acquisition_software,
                        "image_directory": str(file_path.parent),
                        "tag": tilt_series,
                    }
                    if environment.data_collection_parameters:
                        data.update(
                            {
                                "voltage": environment.data_collection_parameters[
                                    "voltage"
                                ],
                                "pixel_size_on_image": environment.data_collection_parameters[
                                    "pixel_size_on_image"
                                ],
                                "image_size_x": environment.data_collection_parameters[
                                    "image_size_x"
                                ],
                                "image_size_y": environment.data_collection_parameters[
                                    "image_size_y"
                                ],
                            }
                        )
                    if environment.data_collection_group_id is None:
                        self._data_collection_stash.append((url, environment, data))
                    else:
                        requests.post(url, json=data)
                    proc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/register_processing_job"
                    if environment.data_collection_ids.get(tilt_series) is None:
                        self._processing_job_stash[tilt_series] = [
                            (
                                proc_url,
                                {"tag": tilt_series, "recipe": "em-tomo-preprocess"},
                            )
                        ]
                        self._processing_job_stash[tilt_series].append(
                            (proc_url, {"tag": tilt_series, "recipe": "em-tomo-align"})
                        )
                    else:
                        if self._processing_job_stash.get(tilt_series):
                            self._flush_processing_job(tilt_series)
                        requests.post(
                            proc_url,
                            json={"tag": tilt_series, "recipe": "em-tomo-preprocess"},
                        )
                        requests.post(
                            proc_url,
                            json={"tag": tilt_series, "recipe": "em-tomo-align"},
                        )
            except Exception as e:
                logger.error(f"ERROR {e}")
        else:
            if file_path not in self._tilt_series[tilt_series]:
                for p in self._tilt_series[tilt_series]:
                    if tilt_angle == extract_tilt_angle(p):
                        break
                else:
                    self._tilt_series[tilt_series].append(file_path)

        if environment and environment.autoproc_program_ids.get(tilt_series):
            preproc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/tomography_preprocess"
            preproc_data = {
                "path": str(file_transferred_to),
                "description": "",
                "size": file_path.stat().st_size,
                "timestamp": file_path.stat().st_ctime,
                "processing_job": environment.processing_job_ids[tilt_series][
                    "em-tomo-preprocess"
                ],
                "data_collection_id": environment.data_collection_ids[tilt_series],
                "image_number": environment.movies[file_transferred_to].movie_number,
                "pixel_size": environment.data_collection_parameters[
                    "pixel_size_on_image"
                ],
                "autoproc_program_id": environment.autoproc_program_ids[tilt_series][
                    "em-tomo-preprocess"
                ],
                "mc_uuid": environment.movies[
                    file_transferred_to
                ].motion_correction_uuid,
                "dose_per_frame": environment.data_collection_parameters.get(
                    "dose_per_frame"
                ),
                "mc_binning": environment.data_collection_parameters.get(
                    "motion_corr_binning", 1
                ),
                "gain_ref": environment.data_collection_parameters.get("gain_ref"),
            }
            requests.post(preproc_url, json=preproc_data)
        elif environment:
            preproc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/tomography_preprocess"
            pfi = ProcessFileIncomplete(
                dest=file_transferred_to,
                source=environment.source,
                image_number=environment.movies[file_transferred_to].movie_number,
                mc_uuid=environment.movies[file_transferred_to].motion_correction_uuid,
                tag=tilt_series,
            )
            if (
                environment.autoproc_program_ids is None
                or environment.processing_job_ids is None
            ) or (
                environment.autoproc_program_ids.get(tilt_series) is None
                or environment.processing_job_ids.get(tilt_series) is None
            ):
                if self._preprocessing_triggers.get(tilt_series):
                    self._preprocessing_triggers[tilt_series].append(
                        (
                            preproc_url,
                            pfi,
                            environment,
                        )
                    )
                else:
                    self._preprocessing_triggers[tilt_series] = [
                        (
                            preproc_url,
                            pfi,
                            environment,
                        )
                    ]

        if self._last_transferred_file:
            last_tilt_series = (
                f"{extract_tilt_tag(self._last_transferred_file)}_{extract_tilt_series(self._last_transferred_file)}"
                if extract_tilt_tag(self._last_transferred_file)
                else extract_tilt_series(self._last_transferred_file)
            )
            last_tilt_angle = extract_tilt_angle(self._last_transferred_file)
            self._last_transferred_file = file_path
            if (
                last_tilt_series != tilt_series and last_tilt_angle != tilt_angle
            ) or self._completed_tilt_series:
                newly_completed_series = []
                if self._tilt_series:
                    tilt_series_size = max(len(ts) for ts in self._tilt_series.values())
                else:
                    tilt_series_size = 0
                this_tilt_series_size = len(self._tilt_series[tilt_series])
                if (
                    this_tilt_series_size >= tilt_series_size
                    and not required_position_files
                ):
                    self._completed_tilt_series.append(tilt_series)
                    newly_completed_series.append(tilt_series)
                for ts, ta in self._tilt_series.items():
                    if required_position_files:
                        completion_test = all(
                            _f.is_file() for _f in required_position_files
                        )
                    else:
                        completion_test = len(ta) >= tilt_series_size
                    if ts not in self._completed_tilt_series and completion_test:
                        newly_completed_series.append(ts)
                        self._completed_tilt_series.append(ts)
                        if environment:
                            file_tilt_list = []
                            movie: str
                            angle: str
                            for movie, angle in environment.tilt_angles[ts]:
                                if environment.motion_corrected_movies.get(Path(movie)):
                                    file_tilt_list.append(
                                        [
                                            str(
                                                environment.motion_corrected_movies[
                                                    Path(movie)
                                                ][0]
                                            ),
                                            angle,
                                            str(
                                                environment.motion_corrected_movies[
                                                    Path(movie)
                                                ][1]
                                            ),
                                        ]
                                    )
                                if environment.motion_corrected_movies.get(
                                    file_transferred_to
                                ):
                                    self._check_for_alignment(
                                        file_transferred_to,
                                        Path(
                                            environment.motion_corrected_movies[  # key error PosixPath
                                                file_transferred_to
                                            ][
                                                0
                                            ]
                                        ),
                                        environment.url.geturl(),
                                        environment.data_collection_ids[ts],
                                        environment.processing_job_ids[ts][
                                            "em-tomo-align"
                                        ],
                                        environment.autoproc_program_ids[ts][
                                            "em-tomo-align"
                                        ],
                                        int(
                                            environment.motion_corrected_movies[
                                                file_transferred_to
                                            ][1]
                                        ),
                                        file_tilt_list,
                                        environment.data_collection_parameters.get(
                                            "tilt_offset"
                                        ),
                                    )
                if newly_completed_series:
                    logger.info(
                        f"The following tilt series are considered complete: {newly_completed_series}"
                    )
                return newly_completed_series
        self._last_transferred_file = file_path
        return []

    def _add_tomo_tilt(
        self,
        file_path: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        required_position_files: List[Path] | None = None,
    ) -> List[str]:
        if environment:
            if tomo_version := environment.software_versions.get("tomo"):
                tilt_info_extraction = tomo_tilt_info.get(tomo_version)
                if not tilt_info_extraction:
                    raise ValueError(
                        f"Extraction routines for TFS Tomo version {tomo_version} unknown"
                    )
            else:
                tilt_info_extraction = tomo_tilt_info["5.7"]
        else:
            tilt_info_extraction = tomo_tilt_info["5.7"]
        tilt_tag = tilt_info_extraction.tag(file_path)
        tilt_series_num = tilt_info_extraction.series(file_path)
        tilt_series = f"{tilt_tag}_{tilt_series_num}" if tilt_tag else tilt_series_num
        return self._add_tilt(
            file_path,
            tilt_info_extraction.series,
            tilt_info_extraction.angle,
            tilt_info_extraction.tag,
            environment=environment,
            required_position_files=required_position_files
            if required_position_files is not None
            else [file_path.parent / (tilt_series + ".mdoc")],
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
            lambda x: "",
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
        if (
            role == "detector"
            and transferred_file.suffix in data_suffixes
            and "gain" not in transferred_file.name
        ):
            if self._acquisition_software == "tomo":
                completed_tilts = self._add_tomo_tilt(
                    transferred_file,
                    environment=environment,
                    required_position_files=kwargs.get("required_position_files"),
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

    def gather_metadata(self, metadata_file: Path) -> OrderedDict:
        if metadata_file.suffix not in (".mdoc", ".xml"):
            raise ValueError(
                f"Tomography gather_metadata method expected xml or mdoc file not {metadata_file.name}"
            )
        if not metadata_file.is_file():
            logger.debug(f"Metadata file {metadata_file} not found")
            return OrderedDict({})
        if metadata_file.suffix == ".xml":
            with open(metadata_file, "r") as xml:
                try:
                    for_parsing = xml.read()
                except Exception:
                    logger.warning(f"Failed to parse file {metadata_file}")
                    return OrderedDict({})
                data = xmltodict.parse(for_parsing)
            metadata: OrderedDict = OrderedDict({})
            metadata["experiment_type"] = TUIFormValue("tomography")
            metadata["voltage"] = TUIFormValue(300)
            metadata["image_size_x"] = TUIFormValue(
                data["Acquisition"]["Info"]["ImageSize"]["Width"]
            )
            metadata["image_size_y"] = TUIFormValue(
                data["Acquisition"]["Info"]["ImageSize"]["Height"]
            )
            metadata["pixel_size_on_image"] = TUIFormValue(
                float(data["Acquisition"]["Info"]["SensorPixelSize"]["Height"])
            )
            metadata["motion_corr_binning"] = TUIFormValue(1)
            metadata["gain_ref"] = TUIFormValue(None, top=True)
            metadata["dose_per_frame"] = TUIFormValue(
                None, top=True, colour="dark_orange"
            )
            metadata["tilt_offset"] = TUIFormValue(0, top=True)
            metadata.move_to_end("gain_ref", last=False)
            metadata.move_to_end("dose_per_frame", last=False)
            # logger.info(f"Metadata extracted from {metadata_file}: {metadata}")
            return metadata
        with open(metadata_file, "r") as md:
            mdoc_data = get_global_data(md)
        if not mdoc_data:
            return OrderedDict({})
        mdoc_metadata: OrderedDict = OrderedDict({})
        mdoc_metadata["experiment_type"] = TUIFormValue("tomography")
        mdoc_metadata["voltage"] = TUIFormValue(float(mdoc_data["Voltage"]))
        mdoc_metadata["image_size_x"] = TUIFormValue(int(mdoc_data["ImageSize"][0]))
        mdoc_metadata["image_size_y"] = TUIFormValue(int(mdoc_data["ImageSize"][1]))
        mdoc_metadata["pixel_size_on_image"] = TUIFormValue(
            float(mdoc_data["PixelSpacing"]) * 1e-10
        )
        mdoc_metadata["motion_corr_binning"] = TUIFormValue(1)
        mdoc_metadata["gain_ref"] = TUIFormValue(None, top=True)
        mdoc_metadata["dose_per_frame"] = TUIFormValue(
            None, top=True, colour="dark_orange"
        )
        mdoc_metadata["tilt_offset"] = TUIFormValue(0, top=True)
        mdoc_metadata.move_to_end("gain_ref", last=False)
        mdoc_metadata.move_to_end("dose_per_frame", last=False)
        # logger.info(f"Metadata extracted from {metadata_file}")
        return mdoc_metadata
