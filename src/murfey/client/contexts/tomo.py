from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, List, NamedTuple, Optional, OrderedDict

import requests
import xmltodict
from pydantic import BaseModel

import murfey.util.eer
from murfey.client.context import Context, ProcessingParameter
from murfey.client.instance_environment import (
    MovieID,
    MovieTracker,
    MurfeyID,
    MurfeyInstanceEnvironment,
    global_env_lock,
)
from murfey.util import authorised_requests, capture_post, get_machine_config
from murfey.util.mdoc import get_block, get_global_data, get_num_blocks

logger = logging.getLogger("murfey.client.contexts.tomo")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


class TiltInfoExtraction(NamedTuple):
    series: Callable[[Path], str]
    angle: Callable[[Path], str]
    tag: Callable[[Path], str]


def _get_tilt_series_v5_7(p: Path) -> str:
    return p.name.split("_")[1]


def _get_tilt_angle_v5_7(p: Path) -> str:
    return p.name.split("[")[1].split("]")[0]


def _get_tilt_tag_v5_7(p: Path) -> str:
    return p.name.split("_")[0]


def _get_slice_index_v5_11(tag: str) -> int:
    slice_index = 0
    for i, ch in enumerate(tag[::-1]):
        if not ch.isnumeric():
            slice_index = -i
            break
    if not slice_index:
        raise ValueError(
            f"The file tag {tag} does not end in numeric characters or is entirely numeric: cannot parse"
        )
    return slice_index


def _get_tilt_series_v5_11(p: Path) -> str:
    tag = p.name.split("_")[0]
    slice_index = _get_slice_index_v5_11(tag)
    return tag[slice_index:]


def _get_tilt_tag_v5_11(p: Path) -> str:
    tag = p.name.split("_")[0]
    slice_index = _get_slice_index_v5_11(tag)
    return tag[:slice_index]


def _get_tilt_angle_v5_11(p: Path) -> str:
    _split = p.name.split("_")[2].split(".")
    return ".".join(_split[:-1])


def _find_angle_index(split_name: List[str]) -> int:
    for i, part in enumerate(split_name):
        if "." in part:
            return i
    return 0


def _get_tilt_series_v5_12(p: Path) -> str:
    split_name = p.name.split("_")
    angle_idx = _find_angle_index(split_name)
    if split_name[angle_idx - 2].isnumeric():
        return split_name[angle_idx - 2]
    return "0"


def _get_tilt_angle_v5_12(p: Path) -> str:
    split_name = p.name.split("_")
    angle_idx = _find_angle_index(split_name)
    return split_name[angle_idx]


def _get_tilt_tag_v5_12(p: Path) -> str:
    split_name = p.name.split("_")
    angle_idx = _find_angle_index(split_name)
    if split_name[angle_idx - 2].isnumeric():
        return "_".join(split_name[: angle_idx - 2])
    return "_".join(split_name[: angle_idx - 1])


tomo_tilt_info = {
    "5.7": TiltInfoExtraction(
        _get_tilt_series_v5_7, _get_tilt_angle_v5_7, _get_tilt_tag_v5_7
    ),
    "5.11": TiltInfoExtraction(
        _get_tilt_series_v5_11, _get_tilt_angle_v5_11, _get_tilt_tag_v5_11
    ),
    "5.12": TiltInfoExtraction(
        _get_tilt_series_v5_12,
        _get_tilt_angle_v5_12,
        _get_tilt_tag_v5_12,
    ),
}


def _construct_tilt_series_name(
    tilt_tag: str, tilt_series: str, file_path: Path
) -> str:
    if tilt_tag:
        if f"{tilt_tag}_{tilt_series}" in file_path.name:
            return f"{tilt_tag}_{tilt_series}"
        return f"{tilt_tag}{tilt_series}"
    return tilt_series


def _midpoint(angles: List[float]) -> int:
    if not angles:
        return 0
    sorted_angles = sorted(angles)
    return round(
        sorted_angles[len(sorted_angles) // 2]
        if sorted_angles[len(sorted_angles) // 2]
        and sorted_angles[len(sorted_angles) // 2 + 1]
        else 0
    )


class ProcessFileIncomplete(BaseModel):
    dest: Path
    source: Path
    image_number: int
    mc_uuid: int
    tag: str
    description: str = ""


class TomographyContext(Context):
    user_params = [
        ProcessingParameter(
            "dose_per_frame", "Dose Per Frame (e- / Angstrom^2 / frame)", default=1
        ),
        ProcessingParameter("manual_tilt_offset", "Tilt Offset", default=0),
        ProcessingParameter("gain_ref", "Gain Reference"),
        ProcessingParameter("eer_fractionation", "EER Fractionation", default=20),
    ]
    metadata_params = [
        ProcessingParameter("voltage", "Voltage"),
        ProcessingParameter("image_size_x", "Image Size X"),
        ProcessingParameter("image_size_y", "Image Size Y"),
        ProcessingParameter("pixel_size_on_image", "Pixel Size"),
        ProcessingParameter("motion_corr_binning", "Motion Correction Binning"),
        ProcessingParameter("num_eer_frames", "Number of EER Frames"),
    ]

    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("Tomography", acquisition_software)
        self._basepath = basepath
        self._tilt_series: Dict[str, List[Path]] = {}
        self._tilt_series_sizes: Dict[str, int] = {}
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
        logger.info(
            f"Flushing {len(self._data_collection_stash)} data collection API calls"
        )
        for dc_data in self._data_collection_stash:
            data = {
                **dc_data[2],
                **{
                    k: v
                    for k, v in dc_data[1].data_collection_parameters.items()
                    if k != "tag"
                },
            }
            capture_post(dc_data[0], json=data)
        self._data_collection_stash = []

    def _flush_processing_job(self, tag: str):
        if proc_data := self._processing_job_stash.get(tag):
            for pd in proc_data:
                requests.post(pd[0], json=pd[1])
            self._processing_job_stash.pop(tag)

    def _flush_processing_jobs(self):
        logger.info(
            f"Flushing {len(self._processing_job_stash.keys())} processing job API calls"
        )
        for v in self._processing_job_stash.values():
            for pd in v:
                requests.post(pd[0], json=pd[1])
        self._processing_job_stash = {}

    def _flush_preprocess(self, tag: str, app_id: int):
        if tag_tr := self._preprocessing_triggers.get(tag):
            for tr in tag_tr:
                process_file = self._complete_process_file(tr[1], tr[2], app_id)
                if process_file:
                    capture_post(tr[0], json=process_file)
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
        manual_tilt_offset: Optional[float],
        pixel_size: Optional[float],
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
                        "manual_tilt_offset": manual_tilt_offset,
                        "pixel_size": pixel_size,
                    }
                    capture_post(url, json=series_data)
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

                eer_fractionation_file = None
                if environment.data_collection_parameters.get("num_eer_frames"):
                    response = requests.post(
                        f"{str(environment.url.geturl())}/visits/{environment.visit}/eer_fractionation_file",
                        json={
                            "num_frames": environment.data_collection_parameters[
                                "num_eer_frames"
                            ],
                            "fractionation": environment.data_collection_parameters[
                                "eer_fractionation"
                            ],
                            "dose_per_frame": environment.data_collection_parameters[
                                "dose_per_frame"
                            ],
                            "fractionation_file_name": "eer_fractionation_tomo.txt",
                        },
                    )
                    eer_fractionation_file = response.json()["eer_fractionation_file"]

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
                    "voltage": environment.data_collection_parameters.get(
                        "voltage", 300
                    ),
                    "eer_fractionation_file": eer_fractionation_file,
                }
                return new_dict
        except KeyError:
            logger.warning("Key error encountered in _complete_process_file")
            return {}

    def _file_transferred_to(
        self, environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
    ):
        machine_config = get_machine_config(
            str(environment.url.geturl()), demo=environment.demo
        )
        if environment.visit in environment.default_destinations[source]:
            return (
                Path(machine_config.get("rsync_basepath", ""))
                / Path(environment.default_destinations[source])
                / file_path.name
            )
        return (
            Path(machine_config.get("rsync_basepath", ""))
            / Path(environment.default_destinations[source])
            / environment.visit
            / file_path.name
        )

    def _get_source(
        self, file_path: Path, environment: MurfeyInstanceEnvironment
    ) -> Path | None:
        for s in environment.sources:
            if file_path.is_relative_to(s):
                return s
        return None

    def _add_tilt(
        self,
        file_path: Path,
        extract_tilt_series: Callable[[Path], str],
        extract_tilt_angle: Callable[[Path], str],
        extract_tilt_tag: Callable[[Path], str],
        environment: MurfeyInstanceEnvironment | None = None,
        required_position_files: List[Path] | None = None,
        required_strings: List[str] | None = None,
    ) -> List[str]:
        if not environment:
            logger.warning("No environment passed in")
            return []
        source = self._get_source(file_path, environment)
        if not source:
            logger.warning(f"No source found for file {file_path}")
            return []
        # required_position_files = required_position_files or []
        required_strings = (
            ["fractions"] if required_strings is None else required_strings
        )
        if required_strings and not any(r in file_path.name for r in required_strings):
            return []
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
                logger.error(f"whoops, {tilt_series_num}, {tilt_angle}")
                return []
            tilt_series = _construct_tilt_series_name(
                tilt_tag, tilt_series_num, file_path
            )

        except Exception:
            logger.debug(
                f"Tilt series and angle could not be determined for {file_path}"
            )
            return []

        if environment:
            file_transferred_to = self._file_transferred_to(
                environment, source, file_path
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
            rerun_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/rerun_tilt_series"
            rerun_data = {
                "client_id": environment.client_id,
                "tag": tilt_series,
                "source": str(file_path.parent),
            }
            capture_post(rerun_url, json=rerun_data)
            if tilt_series in self._aligned_tilt_series:
                with self._lock:
                    self._aligned_tilt_series.remove(tilt_series)

        if not self._tilt_series.get(tilt_series):
            logger.info(f"New tilt series found: {tilt_series}")
            self._tilt_series[tilt_series] = [file_path]
            ts_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/tilt_series"
            ts_data = {
                "client_id": environment.client_id,
                "tag": tilt_series,
                "source": str(file_path.parent),
            }
            capture_post(ts_url, json=ts_data)
            if not self._tilt_series_sizes.get(tilt_series):
                self._tilt_series_sizes[tilt_series] = 0
            try:
                if environment:
                    url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.client_id}/start_data_collection"
                    data = {
                        "experiment_type": "tomography",
                        "file_extension": file_path.suffix,
                        "acquisition_software": self._acquisition_software,
                        "image_directory": str(
                            environment.default_destinations.get(
                                file_path.parent, file_path.parent
                            )
                        ),
                        "data_collection_tag": tilt_series,
                        "source": str(self._basepath),
                        "tag": tilt_series,
                    }
                    if (
                        environment.data_collection_parameters
                        and environment.data_collection_parameters.get("voltage")
                    ):
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
                                "magnification": environment.data_collection_parameters[
                                    "magnification"
                                ],
                            }
                        )
                    proc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.client_id}/register_processing_job"
                    if (
                        environment.data_collection_group_ids.get(str(self._basepath))
                        is None
                    ):
                        self._data_collection_stash.append((url, environment, data))
                        self._processing_job_stash[tilt_series] = [
                            (
                                proc_url,
                                {
                                    "tag": tilt_series,
                                    "recipe": "em-tomo-preprocess",
                                    "experiment_type": "tomography",
                                },
                            )
                        ]
                        self._processing_job_stash[tilt_series].append(
                            (
                                proc_url,
                                {
                                    "tag": tilt_series,
                                    "recipe": "em-tomo-align",
                                    "experiment_type": "tomography",
                                },
                            )
                        )
                    else:
                        capture_post(url, json=data)
                        capture_post(
                            proc_url,
                            json={
                                "tag": tilt_series,
                                "recipe": "em-tomo-preprocess",
                                "experiment_type": "tomography",
                            },
                        )
                        capture_post(
                            proc_url,
                            json={
                                "tag": tilt_series,
                                "recipe": "em-tomo-align",
                                "experiment_type": "tomography",
                            },
                        )

            except Exception as e:
                logger.error(f"ERROR {e}, {environment.data_collection_parameters}")
        else:
            if file_path not in self._tilt_series[tilt_series]:
                for p in self._tilt_series[tilt_series]:
                    if tilt_angle == extract_tilt_angle(p):
                        break
                else:
                    self._tilt_series[tilt_series].append(file_path)

        res = []
        if self._last_transferred_file:
            last_tilt_series = (
                f"{extract_tilt_tag(self._last_transferred_file)}_{extract_tilt_series(self._last_transferred_file)}"
                if extract_tilt_tag(self._last_transferred_file)
                else extract_tilt_series(self._last_transferred_file)
            )
            last_tilt_angle = extract_tilt_angle(self._last_transferred_file)
            self._last_transferred_file = file_path
            if (
                last_tilt_series != tilt_series
                and last_tilt_angle != tilt_angle
                or self._tilt_series_sizes.get(tilt_series)
            ) or self._completed_tilt_series:
                res = self._check_tilt_series(
                    tilt_series,
                    required_position_files or [],
                    file_transferred_to,
                    environment=environment,
                )

        if environment:
            tilt_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.client_id}/tilt"
            tilt_data = {
                "movie_path": str(file_transferred_to),
                "tilt_series_tag": tilt_series,
                "source": str(file_path.parent),
            }
            capture_post(tilt_url, json=tilt_data)

            eer_fractionation_file = None
            if environment.data_collection_parameters.get("num_eer_frames"):
                response = requests.post(
                    f"{str(environment.url.geturl())}/visits/{environment.visit}/eer_fractionation_file",
                    json={
                        "num_frames": environment.data_collection_parameters[
                            "num_eer_frames"
                        ],
                        "fractionation": environment.data_collection_parameters[
                            "eer_fractionation"
                        ],
                        "dose_per_frame": environment.data_collection_parameters[
                            "dose_per_frame"
                        ],
                        "fractionation_file_name": "eer_fractionation_tomo.txt",
                    },
                )
                eer_fractionation_file = response.json()["eer_fractionation_file"]
            preproc_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.client_id}/tomography_preprocess"
            preproc_data = {
                "path": str(file_transferred_to),
                "description": "",
                "data_collection_id": environment.data_collection_ids.get(tilt_series),
                "image_number": environment.movies[file_transferred_to].movie_number,
                "pixel_size": environment.data_collection_parameters.get(
                    "pixel_size_on_image", 0
                ),
                "autoproc_program_id": environment.autoproc_program_ids.get(
                    tilt_series, {}
                ).get("em-tomo-preprocess"),
                "dose_per_frame": environment.data_collection_parameters.get(
                    "dose_per_frame", 0
                ),
                "mc_binning": environment.data_collection_parameters.get(
                    "motion_corr_binning", 1
                ),
                "gain_ref": environment.data_collection_parameters.get("gain_ref"),
                "voltage": environment.data_collection_parameters.get("voltage", 300),
                "eer_fractionation_file": eer_fractionation_file,
                "tag": tilt_series,
                "group_tag": str(self._basepath),
            }
            capture_post(preproc_url, json=preproc_data)

        self._last_transferred_file = file_path
        return res

    def _check_tilt_series(
        self,
        tilt_series: str,
        required_position_files: List[Path],
        file_transferred_to: Path | None,
        environment: MurfeyInstanceEnvironment | None = None,
    ) -> List[str]:
        newly_completed_series: List[str] = []
        if not self._tilt_series:
            return newly_completed_series
        this_tilt_series_size = len(self._tilt_series.get(tilt_series, []))
        tilt_series_size_check = (
            (this_tilt_series_size == self._tilt_series_sizes.get(tilt_series))
            if self._tilt_series_sizes.get(tilt_series)
            else False
        )
        if tilt_series_size_check and not required_position_files:
            if tilt_series not in self._completed_tilt_series:
                self._completed_tilt_series.append(tilt_series)
                newly_completed_series.append(tilt_series)
        for ts, ta in self._tilt_series.items():
            required_position_files_check = (
                all(_f.is_file() for _f in required_position_files)
                if required_position_files
                else True
            )
            if self._tilt_series_sizes.get(ts):
                completion_test = len(ta) >= self._tilt_series_sizes[ts]
                if completion_test:
                    completion_test = required_position_files_check
            else:
                completion_test = False
            if ts not in self._completed_tilt_series and completion_test:
                newly_completed_series.append(ts)
                self._completed_tilt_series.append(ts)
                if environment and file_transferred_to:
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
                        if environment.motion_corrected_movies.get(file_transferred_to):
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
                                environment.processing_job_ids[ts]["em-tomo-align"],
                                environment.autoproc_program_ids[ts]["em-tomo-align"],
                                int(
                                    environment.motion_corrected_movies[
                                        file_transferred_to
                                    ][1]
                                ),
                                file_tilt_list,
                                environment.data_collection_parameters.get(
                                    "manual_tilt_offset"
                                ),
                                environment.data_collection_parameters.get(
                                    "pixel_size_on_image"
                                ),
                            )
        return newly_completed_series

    def _add_tomo_tilt(
        self,
        file_path: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        required_position_files: List[Path] | None = None,
        required_strings: List[str] | None = None,
    ) -> List[str]:
        required_strings = (
            ["fractions"] if required_strings is None else required_strings
        )
        if not any(r in file_path.name for r in required_strings):
            return []
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
        tilt_series = _construct_tilt_series_name(tilt_tag, tilt_series_num, file_path)
        return self._add_tilt(
            file_path,
            tilt_info_extraction.series,
            tilt_info_extraction.angle,
            tilt_info_extraction.tag,
            environment=environment,
            required_position_files=(
                required_position_files
                if required_position_files is not None
                else [file_path.parent / (tilt_series + ".mdoc")]
            ),
            required_strings=required_strings,
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
            required_strings=[],
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
        if role == "detector" and "gain" not in transferred_file.name:
            if transferred_file.suffix in data_suffixes:
                if self._acquisition_software == "tomo":
                    if environment:
                        machine_config = get_machine_config(
                            str(environment.url.geturl()), demo=environment.demo
                        )
                    else:
                        machine_config = {}
                    required_strings = (
                        machine_config.get("data_required_substrings", {})
                        .get("tomo", {})
                        .get(transferred_file.suffix, ["fractions"])
                    )
                    completed_tilts = self._add_tomo_tilt(
                        transferred_file,
                        environment=environment,
                        required_position_files=kwargs.get("required_position_files"),
                        required_strings=kwargs.get("required_strings")
                        or required_strings,
                    )
                elif self._acquisition_software == "serialem":
                    completed_tilts = self._add_serialem_tilt(
                        transferred_file, environment=environment
                    )
            if transferred_file.suffix == ".mdoc":
                with open(transferred_file, "r") as md:
                    tilt_series = transferred_file.stem
                    self._tilt_series_sizes[tilt_series] = get_num_blocks(md)
                if environment:
                    source = self._get_source(transferred_file, environment)
                    if source:
                        completed_tilts = self._check_tilt_series(
                            tilt_series,
                            kwargs.get("required_position_files") or [],
                            self._file_transferred_to(
                                environment, source, transferred_file
                            ),
                            environment=environment,
                        )

                    # Always update the tilt series length in the database after an mdoc
                    if environment.murfey_session is not None:
                        length_url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/tilt_series_length"
                        capture_post(
                            length_url,
                            json={
                                "tags": [tilt_series],
                                "source": str(transferred_file.parent),
                                "tilt_series_lengths": [
                                    self._tilt_series_sizes[tilt_series]
                                ],
                            },
                        )

        if completed_tilts and environment:
            logger.info(
                f"The following tilt series are considered complete: {completed_tilts} "
                f"after {transferred_file}"
            )
            complete_url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.client_id}/completed_tilt_series"
            capture_post(
                complete_url,
                json={
                    "tags": completed_tilts,
                    "source": str(transferred_file.parent),
                    "tilt_series_lengths": [
                        len(self._tilt_series.get(ts, [])) for ts in completed_tilts
                    ],
                },
            )
        return completed_tilts

    def post_first_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        self.post_transfer(
            transferred_file, role=role, environment=environment, **kwargs
        )

    def gather_metadata(
        self, metadata_file: Path, environment: MurfeyInstanceEnvironment | None = None
    ) -> OrderedDict:
        if metadata_file.suffix not in (".mdoc", ".xml"):
            raise ValueError(
                f"Tomography gather_metadata method expected xml or mdoc file not {metadata_file.name}"
            )
        try:
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
                try:
                    metadata: OrderedDict = OrderedDict({})
                    metadata["experiment_type"] = "tomography"
                    metadata["voltage"] = 300
                    metadata["image_size_x"] = data["Acquisition"]["Info"]["ImageSize"][
                        "Width"
                    ]
                    metadata["image_size_y"] = data["Acquisition"]["Info"]["ImageSize"][
                        "Height"
                    ]
                    metadata["pixel_size_on_image"] = float(
                        data["Acquisition"]["Info"]["SensorPixelSize"]["Height"]
                    )
                    metadata["motion_corr_binning"] = 1
                    metadata["gain_ref"] = None
                    metadata["dose_per_frame"] = (
                        environment.data_collection_parameters.get("dose_per_frame")
                        if environment
                        else None
                    )
                    metadata["manual_tilt_offset"] = 0
                    metadata["source"] = str(self._basepath)
                except KeyError:
                    return OrderedDict({})
                return metadata
            with open(metadata_file, "r") as md:
                mdoc_data = get_global_data(md)
                num_blocks = get_num_blocks(md)
                md.seek(0)
                blocks = [get_block(md) for i in range(num_blocks)]
                mdoc_data_block = blocks[0]
            if not mdoc_data:
                return OrderedDict({})
            mdoc_metadata: OrderedDict = OrderedDict({})
            mdoc_metadata["experiment_type"] = "tomography"
            mdoc_metadata["voltage"] = float(mdoc_data["Voltage"])
            mdoc_metadata["image_size_x"] = int(mdoc_data["ImageSize"][0])
            mdoc_metadata["image_size_y"] = int(mdoc_data["ImageSize"][1])
            mdoc_metadata["magnification"] = int(mdoc_data_block["Magnification"])
            superres_binning = int(mdoc_data_block["Binning"])
            binning_factor = 1
            if environment:
                server_config = requests.get(
                    f"{str(environment.url.geturl())}/machine/"
                ).json()
                if (
                    server_config.get("superres")
                    and superres_binning == 1
                    and environment.superres
                ):
                    binning_factor = 2
                ps_from_mag = (
                    server_config.get("calibrations", {})
                    .get("magnification", {})
                    .get(mdoc_data_block["Magnification"])
                )
                if ps_from_mag:
                    mdoc_metadata["pixel_size_on_image"] = (
                        float(ps_from_mag) * 1e-10 / binning_factor
                    )
            if mdoc_metadata.get("pixel_size_on_image") is None:
                mdoc_metadata["pixel_size_on_image"] = (
                    float(mdoc_data["PixelSpacing"]) * 1e-10
                )
            mdoc_metadata["motion_corr_binning"] = binning_factor
            if environment:
                mdoc_metadata["gain_ref"] = (
                    environment.data_collection_parameters.get("gain_ref")
                    if environment.data_collection_parameters.get("gain_ref")
                    not in (None, "None")
                    else f"data/{datetime.now().year}/{environment.visit}/processing/gain.mrc"
                )
            else:
                mdoc_metadata["gain_ref"] = None
            mdoc_metadata["dose_per_frame"] = (
                environment.data_collection_parameters.get("dose_per_frame")
                if environment
                else None
            )
            mdoc_metadata["manual_tilt_offset"] = -_midpoint(
                [float(b["TiltAngle"]) for b in blocks]
            )
            mdoc_metadata["source"] = str(self._basepath)
            mdoc_metadata["tag"] = str(self._basepath)
            mdoc_metadata["tilt_series_tag"] = metadata_file.stem
            mdoc_metadata["exposure_time"] = float(mdoc_data_block["ExposureTime"])
            mdoc_metadata["slit_width"] = float(mdoc_data_block["FilterSlitAndLoss"][0])
            mdoc_metadata["file_extension"] = (
                f".{mdoc_data_block['SubFramePath'].split('.')[-1]}"
            )
            mdoc_metadata["eer_fractionation"] = (
                environment.data_collection_parameters.get("eer_fractionation")
                if environment
                else None
            ) or 20

            data_file = mdoc_data_block["SubFramePath"].split("\\")[-1]
            if data_file.split(".")[-1] == "eer":
                mdoc_metadata["num_eer_frames"] = murfey.util.eer.num_frames(
                    metadata_file.parent / data_file
                )
        except Exception as e:
            logger.error(f"Exception encountered in metadata gathering: {str(e)}")
            return OrderedDict({})

        return mdoc_metadata
